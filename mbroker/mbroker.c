#include "logging.h"
#include "string.h"
#include "wire_protocol.h"

#include "operations.h"
#include "state.h"

#include "pthread.h"
#include "errno.h"

#include "producer-consumer.h"
typedef struct
{
  char *name;
  ssize_t size;

  uint64_t subs;
  uint64_t pubs;

  pthread_mutex_t pcq_publisher_condvar_lock;
  pthread_cond_t pcq_publisher_condvar;

  pthread_cond_t pcq_subscriber_condvar;
} BoxData;

typedef struct
{
  BoxData **boxes;
  long unsigned int box_count;
} State;

State *server_state;

// init global state
int initServerState()
{
  server_state = (State *)malloc(sizeof(State));

  if (server_state == NULL)
    return -1;

  server_state->box_count = 0;
  return 0;
}

BoxData *initBox(char *box_name)
{
  BoxData *box = (BoxData *)malloc(sizeof(BoxData));

  box->name = (char *)malloc(sizeof(char) * (strlen(box_name) + 1));

  if (box->name == NULL)
    return NULL;

  strcpy(box->name, box_name);

  box->size = 0;
  box->subs = 0;
  box->pubs = 0;

  if (pthread_mutex_init(&box->pcq_publisher_condvar_lock, NULL) != 0)
    return NULL;

  if (pthread_cond_init(&box->pcq_publisher_condvar, NULL) != 0)
    return NULL;

  if (pthread_cond_init(&box->pcq_subscriber_condvar, NULL) != 0)
    return NULL;

  return box;
}

BoxData *getBox(char *box_name)
{
  for (int i = 0; i < server_state->box_count; i++)
  {
    if (strcmp(server_state->boxes[i]->name, box_name) == 0)
      return server_state->boxes[i];
  }

  return NULL;
}

int handlePublisher(char *client_pipe_name, char *box_name)
{
  // TODO how to tell server that return was -1 ?

  // TODO: if a publisher or a subscriber try to connect to a box that doesnt exist, what happen?
  // currently returns -1;
  BoxData *box = getBox(box_name);

  if (box == NULL)
  {
    // TODO unlink fifo to signal error
    unlink(client_pipe_name);
    printf("Box doesnt exist\n");
    return -1;
  }

  // lock publisher mutex
  if (pthread_mutex_lock(&box->pcq_publisher_condvar_lock) != 0)
  {
    printf("Error locking mutex\n");
    return -1;
  }

  // TODO: only 1 publisher at a time is supposed to be connected to the box, so if a publisher tries to connect to a box and a publisher is already connected what happen?
  // if more than one publisher is connected to a box
  // wait for publisher to disconnect
  while (box->pubs != 0)
  {
    if (pthread_cond_wait(&box->pcq_publisher_condvar, &box->pcq_publisher_condvar_lock) != 0)
      return -1;
  }

  box->pubs++;

  // Unlock publisher mutex
  if (pthread_mutex_unlock(&box->pcq_publisher_condvar_lock) != 0)
  {
    printf("Error unlocking mutex\n");
    box->pubs--;
    // broadcast pubs decrement
    pthread_cond_broadcast(&box->pcq_publisher_condvar);
    return -1;
  }

  char updatedBoxName[BOX_NAME_SIZE + 1] = "/";

  strcat(updatedBoxName, box->name);

  // connect to publisher
  int client_fifo = open(client_pipe_name, O_RDONLY);
  printf("[Publisher connected]\n");

  int fhandle = -1;

  // read from publisher fifo
  char buffer[PROTOCOL_MESSAGE_SIZE];
  while (1)
  {
    if (read(client_fifo, buffer, PROTOCOL_MESSAGE_SIZE) == 0)
      break;

    // parse wire message received
    OP_CODE_SIZE message_op_code;
    char message[MESSAGE_SIZE];
    sscanf(buffer, "%hhd|%[^\n]", &message_op_code, message);

    size_t message_len = strlen(message);

    char buffer_to_write[message_len + 1];
    strcpy(buffer_to_write, message);
    buffer_to_write[message_len] = '\0';

    printf("Publisher: %s\n", buffer_to_write);

    // needs to be open every time to adjust offset to start writing in correct spot
    fhandle = tfs_open(updatedBoxName, TFS_O_APPEND);

    if (fhandle == -1)
    {
      printf("Error opening box %s\n", box->name);
      break;
    }

    //  TODO: write the message in box located in tfs
    // DONE
    ssize_t bytes_written = tfs_write(fhandle, buffer_to_write, message_len + 1);

    if (bytes_written == -1)
    {
      // TODO check error handling while writing to box
      printf("Error writing to box %s\n", box->name);
      break;
    }

    box->size += bytes_written;

    // broadcast change in box subs
    if (pthread_cond_broadcast(&box->pcq_subscriber_condvar) != 0)
      return -1;

    if (tfs_close(fhandle) == -1)
    {
      printf("Eror closing file\n");
      break;
    }
  }

  // close box
  // no error checking because if error still need to run command below
  if (fhandle != -1)
  {
    tfs_close(fhandle);
  }

  // close fifo
  close(client_fifo);

  // TODO: when publisher disconnects from the box, we need to decrement the n_publishers variable in the box_data
  // DONE
  printf("[Publisher disconnected]\n");
  box->pubs--;

  // broadcast change in box pubs
  if (pthread_cond_broadcast(&box->pcq_publisher_condvar) != 0)
    return -1;

  return 0;
}

int handleSubscriber(char *client_pipe_name, char *box_name)
{
  // Find the specified box
  BoxData *box = getBox(box_name);
  if (box == NULL)
  {
    // TODO unlink pipe to signal error
    unlink(client_pipe_name);
    printf("Error: Box %s does not exist.\n", box_name);
    return -1;
  }

  box->subs++;

  char updatedBoxName[BOX_NAME_SIZE + 1] = "/";

  strcat(updatedBoxName, box->name);

  int fhandle = -1;

  ssize_t bytes_read;
  ssize_t last_bytes_read = 0;

  pthread_mutex_t box_subscriber;

  if (pthread_mutex_init(&box_subscriber, NULL) != 0)
  {
    printf("Error initializing mutex\n");
    return -1;
  }

  // connect to publisher
  int client_fifo = open(client_pipe_name, O_WRONLY);
  printf("[Subscriber connected]\n");
  char buffer[PROTOCOL_MESSAGE_SIZE];

  // TODO: disconnect subscriber
  // TODO: change every return -1 to variable error.. and return error in the end
  while (access(client_pipe_name, F_OK) != -1)
  {
    if (pthread_mutex_lock(&box_subscriber) != 0)
    {
      printf("Error locking mutex\n");
      return -1;
    }

    int res = 0;

    while(box->size == last_bytes_read)
    {
      // wait for publisher to write to box
      struct timespec ts;
      clock_gettime(CLOCK_REALTIME, &ts);
      ts.tv_sec += 1; // 1s

      // time out limit to check if subscriber is still connected
      res = pthread_cond_timedwait(&box->pcq_subscriber_condvar, &box_subscriber, &ts);
      if (res == ETIMEDOUT)
      {
        break;
      }
      else if (res != 0)
      {
        break;
      }
      else printf("quick exit\n");
    }

    if(res == ETIMEDOUT)
    {
      if (pthread_mutex_unlock(&box_subscriber) != 0)
      {
        printf("Error unlocking mutex\n");
        return -1;
      }

      continue;
    }

    if (pthread_mutex_unlock(&box_subscriber) != 0)
    {
      printf("Error unlocking mutex\n");
      return -1;
    }

    // needs to be open every time to adjust offset to start writing in correct spot
    fhandle = tfs_open(updatedBoxName, 0);

    if (fhandle == -1)
    {
      printf("Error opening box %s\n", box->name);
      break;
    }

    bytes_read = tfs_read(fhandle, buffer, PROTOCOL_MESSAGE_SIZE);
    if (bytes_read == -1)
    {
      // TODO check error handling while writing to box
      printf("Error reading from box %s\n", box->name);
      break;
    }

    // TODO: check why the subscribers are receiving the last 2 messages instead of the last one
    char message[MESSAGE_SIZE];
    for (int i = 0; i < bytes_read - last_bytes_read; i++)
    {
      if (buffer[last_bytes_read + i] == '\0')
      {
        message[i] = '\0';
        last_bytes_read += i + 1;
        break;
      }
      else
      {
        message[i] = buffer[last_bytes_read + i];
      }
    }

    printf("Sending to subscriber: %s\n", message);

    // TODO: abstract this to a function
    char wire_message[PROTOCOL_MESSAGE_SIZE];

    sprintf(wire_message, "%d|%s", SEND_SUBSCRIBER, message);

    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error writing to client fifo\n");
      break;
    }

    if (tfs_close(fhandle) == -1)
    {
      printf("Eror closing file\n");
      break;
    }
  }

  printf("[Subscriber disconnected]\n");

  // close box
  // no error checking because if error still need to run command below
  if (fhandle != -1)
    tfs_close(fhandle);

  // close fifo
  close(client_fifo);
  box->subs--;

  return 0;
}

int createBox(char *client_pipe_name, char *box_name)
{
  char wire_message[PROTOCOL_MESSAGE_SIZE];

  // format string for tfs
  char box_name_update[BOX_NAME_SIZE + 1] = "/";

  // TODO: check if box already exist (idk how since we dont have access to tfs_lookup)
  // DONE

  // probably need to extend tfs api
  strcat(box_name_update, box_name);

  // create box in tfs open
  int fhandle = tfs_open(box_name_update, TFS_O_CREAT);

  if (fhandle == -1 || tfs_close(fhandle) == -1 || getBox(box_name) != NULL)
    // build ERROR response
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s", RETURN_CREATE_BOX, -1, "Error creating box");
  else
    // build OK response
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s", RETURN_CREATE_BOX, 0, "\0");

  // TODO: check if the client pipe exist to receive the response

  // open client pipe
  int client_fifo = open(client_pipe_name, O_WRONLY);

  // write to client pipe
  // TODO: come up with a better way of sending the response back since more errors can occur bellow
  if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
  {
    printf("Error while writing to client fifo\n");
    close(client_fifo);
    return -1;
  };

  // close client pipe
  close(client_fifo);

  // create box structure
  BoxData *box = (BoxData *)malloc(sizeof(BoxData));
  box->subs = 0;
  box->pubs = 0;

  if (box == NULL)
    return -1;

  // allocate memory for the box name
  box->name = (char *)malloc(sizeof(char) * (strlen(box_name) + 1));
  if (box->name == NULL)
    return -1;
  strcpy(box->name, box_name);

  // add box to the server state
  // TODO: check if it's better to implement a different add/remove reallocation by starting with a default dynamic array size and doubling it when needed, reducing the use of realloc
  server_state->box_count++;

  server_state->boxes = realloc(server_state->boxes, sizeof(BoxData *) * server_state->box_count);
  if (server_state->boxes == NULL)
    return -1;

  // insert the newly created box
  server_state->boxes[server_state->box_count - 1] = box;

  return 0;
}

int listBoxes(char *client_pipe_name)
{
  char wire_message[PROTOCOL_MESSAGE_SIZE];

  // TODO: check if client exists before trying to send the list
  // open client pipe

  // check if no box was created
  if (server_state->box_count == 0)
  {
    // write message
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s|%d|%d|%d", RETURN_LIST_BOXES, 1, "\0", 0, 0, 0);

    // write to client pipe
    int client_fifo = open(client_pipe_name, O_WRONLY);
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while writing to client fifo");
      close(client_fifo);
      return -1;
    };

    // close client pipe
    close(client_fifo);
    return 0;
  }

  // open client fifo
  int client_fifo = open(client_pipe_name, O_WRONLY);

  for (int i = 0; i < server_state->box_count; i++)
  {

    BoxData *box_data = server_state->boxes[i];

    char *name = box_data->name;
    uint64_t pub = box_data->pubs;
    uint64_t sub = box_data->subs;

    // format string for tfs
    char box_name_update[BOX_NAME_SIZE + 1] = "/";
    strcat(box_name_update, name);

    int fhandle = tfs_open(box_name_update, 0);
    if (fhandle == -1)
    {
      printf("Error while getting file bytes");
      close(client_fifo);
      return -1;
    }

    char buffer[MESSAGE_SIZE];
    BOX_SIZE box_size = (BOX_SIZE)tfs_read(fhandle, buffer, 1024);

    if (box_size == -1)
    {
      printf("Error while getting file bytes");
      close(client_fifo);
      return -1;
    }

    // parse wire message
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%hhd|%hhd|%s|%ld|%ld|%ld", RETURN_LIST_BOXES, i == server_state->box_count - 1, name, box_size, pub, sub);

    // write to client pipe
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while writing to client fifo");
      close(client_fifo);
      return -1;
    };
  }

  // close client fifo
  close(client_fifo);
  return 0;
}

int deleteBox(char *client_pipe_name, char *box_name)
{
  char wire_message[PROTOCOL_MESSAGE_SIZE];
  int box_index = -1;

  // check if box exists
  for (int i = 0; i < server_state->box_count; i++)
  {
    if (strcmp(server_state->boxes[i]->name, box_name) == 0)
    {
      box_index = i;
      break;
    }
  }

  if (box_index == -1)
  {
    // build ERROR response
    // TODO: signal error to client if box doesnt exist
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s", RETURN_DELETE_BOX, -1, "Error: Box does not exist");

    // write to client pipe
    int client_fifo = open(client_pipe_name, O_WRONLY);
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while writing to client fifo");
      close(client_fifo);
      return -1;
    }

    // close client pipe
    close(client_fifo);

    return 0;
  }

  // delete box from tfs
  char box_name_update[BOX_NAME_SIZE + 1] = "/";
  strcat(box_name_update, box_name);

  if (tfs_unlink(box_name_update) == -1)
  {
    // build ERROR response
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s", RETURN_DELETE_BOX, -1, "Error deleting box from TFS");

    // write to client pipe
    int client_fifo = open(client_pipe_name, O_WRONLY);
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while writing to client fifo");
      close(client_fifo);
      return -1;
    }

    // close client pipe
    close(client_fifo);

    return -1;
  }

  // free memory allocated for the box name
  free(server_state->boxes[box_index]->name);

  // free memory allocated for the box data structure
  free(server_state->boxes[box_index]);

  // remove the box from the server state
  for (int i = box_index; i < server_state->box_count - 1; i++)
  {
    server_state->boxes[i] = server_state->boxes[i + 1];
  }
  server_state->box_count--;

  // build OK response
  snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s", RETURN_DELETE_BOX, 0, "\0");

  // write to client pipe
  int client_fifo = open(client_pipe_name, O_WRONLY);
  if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
  {
    printf("Error while writing to client fifo");
    close(client_fifo);
    return -1;
  }

  // close client pipe
  close(client_fifo);

  return 0;
}

void session(OP_CODE_SIZE op_code, char *client_pipe_name, char *box_name)
{
  switch (op_code)
  {
  case REGISTER_PUBLISHER:
    handlePublisher(client_pipe_name, box_name);
    break;

  case REGISTER_SUBSCRIBER:
    handleSubscriber(client_pipe_name, box_name);
    break;

  case CREATE_BOX:
    createBox(client_pipe_name, box_name);
    break;

  case DELETE_BOX:
    deleteBox(client_pipe_name, box_name);
    break;

  case LIST_BOXES:
    listBoxes(client_pipe_name);
    break;

  default:
    break;
  }
}

void thread_function(pc_queue_t *pcq, int thread_id)
{
  printf("Initializing thread\n");
  while (1)
  {
    // get register message from producer-consumer queue
    char *register_message = pcq_dequeue(pcq);

    printf("[THREAD %d] handling register\n", thread_id);

    // parse register message
    OP_CODE_SIZE op_code;
    char client_pipe_name[PIPE_NAME_SIZE];
    char box_name[BOX_NAME_SIZE];
    sscanf(register_message, "%hhd|%[^|]|%s", &op_code, client_pipe_name, box_name);

    // free memory allocated for register message
    // free(register_message);

    // handle session
    session(op_code, client_pipe_name, box_name);
    printf("[THREAD %d] closed session\n", thread_id);
  }
}

int main(int argc, char **argv)
{
  if (argc != 3)
  {
    printf("too few arguments");
    return -1;
  }

  char *register_pipe_name = argv[1];
  size_t n_sessions = (size_t)atoi(argv[2]); // used when multi-threading

  // create the register pipe
  if (mkfifo(register_pipe_name, 0666) == -1)
  {
    perror("error creating register pipe");
    return -1;
  }

  // init tfs
  printf("Creating file system\n");
  if (tfs_init(NULL) == -1)
  {
    printf("Error creating file system");
    return -1;
  };

  if (initServerState() == -1)
  {
    printf("Error initializing server state");
    return -1;
  }

  printf("Started listening for client connections..\n");

  // init n_sessions threads
  pc_queue_t pcq;

  pcq_create(&pcq, n_sessions);

  for (int i = 0; i < n_sessions; i++)
  {
    pthread_t thread;
    pthread_create(&thread, NULL, (void *)thread_function, &pcq);
  }

  // receive register messages
  while (1)
  {
    // open the register pipe
    int register_fifo = open(register_pipe_name, O_RDONLY);

    // read from the register pipe
    char buffer[PROTOCOL_MESSAGE_SIZE];
    if (read(register_fifo, buffer, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while reading register fifo");
      return -1;
    };

    // enqueue register message
    pcq_enqueue(&pcq, buffer);

    close(register_fifo);
  }

  return 0;
}