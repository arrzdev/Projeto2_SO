#include "logging.h"
#include "client.h"

#include "operations.h"
#include "state.h"

#include "pthread.h"
#include "errno.h"

#include "producer-consumer.h"

#include "signal.h"
#include "errno.h"
volatile sig_atomic_t exit_flag = 0;

static void handleSIGINT(int sig)
{
  (void)sig;
  exit_flag = 1;
}
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
  BoxData *box = getBox(box_name);

  if (box == NULL)
  {
    unlink(client_pipe_name);
    WARN("Box doesnt exist\n");
    return -1;
  }

  // lock publisher mutex
  if (pthread_mutex_lock(&box->pcq_publisher_condvar_lock) != 0)
  {
    unlink(client_pipe_name);
    WARN("Error lock mutex: %s\n", strerror(errno));
    return -1;
  }

  // if more than one publisher is connected to a box
  // wait for publisher to disconnect
  while (box->pubs != 0)
  {
    if (pthread_cond_wait(&box->pcq_publisher_condvar, &box->pcq_publisher_condvar_lock) != 0)
    {
      unlink(client_pipe_name);
      WARN("Error waiting mutex: %s\n", strerror(errno));
      return -1;
    }
  }

  box->pubs++;

  // Unlock publisher mutex
  if (pthread_mutex_unlock(&box->pcq_publisher_condvar_lock) != 0)
  {
    unlink(client_pipe_name);
    WARN("Error unlock mutex: %s\n", strerror(errno));
    box->pubs--;

    // broadcast pubs decrement
    if (pthread_cond_broadcast(&box->pcq_publisher_condvar) != 0)
    {
      WARN("Error broadcasting mutex: %s\n", strerror(errno));
    }

    return -1;
  }

  char updatedBoxName[BOX_NAME_SIZE + 1] = "/";

  strcat(updatedBoxName, box->name);

  // connect to publisher
  int client_fifo = open(client_pipe_name, O_RDONLY);

  int fhandle = -1;

  bool error = false;

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

    // needs to be open every time to adjust offset to start writing in correct spot
    fhandle = tfs_open(updatedBoxName, TFS_O_APPEND);

    if (fhandle == -1)
    {
      WARN("Error opening box: %s\n", box->name);
      error = true;
      break;
    }

    ssize_t bytes_written = tfs_write(fhandle, buffer_to_write, message_len + 1);

    if (bytes_written == -1)
    {
      WARN("Error writing to box %s\n", box->name);
      error = true;

      if (tfs_close(fhandle) == -1)
        WARN("Error closing box %s\n", box->name);

      break;
    }

    box->size += bytes_written;

    // broadcast change in box subs
    if (pthread_cond_broadcast(&box->pcq_subscriber_condvar) != 0)
    {
      WARN("Error broadcasting mutex: %s\n", strerror(errno));
      error = true;
      break;
    }

    if (tfs_close(fhandle) == -1)
    {
      WARN("Error closing box %s\n", box->name);
      error = true;
      break;
    }
  }

  // close box
  // no error checking because if error still need to run command below
  if (fhandle != -1)
  {
    if (tfs_close(fhandle) == -1)
    {
      WARN("Error closing box %s\n", box->name);
      error = true;
    }
  }

  // close fifo
  if (close(client_fifo) == -1)
  {
    WARN("Error closing fifo %s\n", client_pipe_name);
    error = true;
  }

  box->pubs--;

  // broadcast change in box pubs
  if (pthread_cond_broadcast(&box->pcq_publisher_condvar) != 0)
  {
    WARN("Error broadcasting mutex: %s\n", strerror(errno));
    error = true;
  }

  if (error)
  {
    unlink(client_pipe_name);
    return -1;
  }

  return 0;
}

int handleSubscriber(char *client_pipe_name, char *box_name)
{
  // Find the specified box
  BoxData *box = getBox(box_name);
  if (box == NULL)
  {
    unlink(client_pipe_name);
    WARN("Error: Box %s does not exist.\n", box_name);
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
    WARN("Error init mutex: %s\n", strerror(errno));
    return -1;
  }

  // connect to publisher
  int client_fifo = open(client_pipe_name, O_WRONLY);
  char buffer[PROTOCOL_MESSAGE_SIZE];

  bool error = false;

  while (access(client_pipe_name, F_OK) != -1)
  {
    if (pthread_mutex_lock(&box_subscriber) != 0)
    {
      WARN("Error lock mutex: %s\n", strerror(errno));
      error = true;
      break;
    }

    int res = 0;

    while (box->size == last_bytes_read)
    {
      // wait for publisher to write to box
      struct timespec ts;
      clock_gettime(CLOCK_REALTIME, &ts);
      ts.tv_sec += 1; // 1s

      // time out limit to check if subscriber is still connected
      res = pthread_cond_timedwait(&box->pcq_subscriber_condvar, &box_subscriber, &ts);
      if (res != 0)
        break;
    }

    if (res == ETIMEDOUT)
    {
      if (pthread_mutex_unlock(&box_subscriber) != 0)
      {
        WARN("Error unlock mutex: %s\n", strerror(errno));
        error = true;
        break;
      }

      continue;
    }

    if (pthread_mutex_unlock(&box_subscriber) != 0)
    {
      WARN("Error unlock mutex: %s\n", strerror(errno));
      error = true;
      break;
    }

    // needs to be open every time to adjust offset to start writing in correct spot
    fhandle = tfs_open(updatedBoxName, 0);

    if (fhandle == -1)
    {
      WARN("Error opening box: %s\n", box->name);
      error = true;
      break;
    }

    bytes_read = tfs_read(fhandle, buffer, PROTOCOL_MESSAGE_SIZE);
    if (bytes_read == -1)
    {
      WARN("Error reading box: %s\n", box->name);
      error = true;
      break;
    }

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

    char wire_message[PROTOCOL_MESSAGE_SIZE];

    sprintf(wire_message, "%d|%s", SEND_SUBSCRIBER, message);

    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      WARN("Error writing to fifo %s\n", client_pipe_name);
      error = true;
      break;
    }

    if (tfs_close(fhandle) == -1)
    {
      WARN("Error closing box %s\n", box->name);
      error = true;
      break;
    }
  }

  // close box
  // no error checking because if error still need to run command below
  if (fhandle != -1)
  {
    if (tfs_close(fhandle) == -1)
    {
      WARN("Error closing box %s\n", box->name);
      error = true;
    }
  }

  // close fifo
  if (close(client_fifo) == -1)
  {
    WARN("Error closing fifo %s\n", client_pipe_name);
    error = true;
  }

  box->subs--;

  if (error)
  {
    unlink(client_pipe_name);
    return -1;
  }

  return 0;
}

int createBox(char *client_pipe_name, char *box_name)
{
  char wire_message[PROTOCOL_MESSAGE_SIZE];

  // format string for tfs
  char box_name_update[BOX_NAME_SIZE + 1] = "/";

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

  // open client pipe
  int client_fifo = open(client_pipe_name, O_WRONLY);

  if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
  {
    WARN("Error while writing to client fifo\n");
    if (close(client_fifo) == -1)
      WARN("Error closing fifo %s\n", client_pipe_name);
    return -1;
  };

  // close client pipe
  if (close(client_fifo) == -1)
    WARN("Error closing fifo %s\n", client_pipe_name);

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

  // check if no box was created
  if (server_state->box_count == 0)
  {
    // write message
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s|%d|%d|%d", RETURN_LIST_BOXES, 1, "\0", 0, 0, 0);

    // write to client pipe
    int client_fifo = open(client_pipe_name, O_WRONLY);
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      WARN("Error while writing to client fifo");
      if (close(client_fifo) == -1)
        WARN("Error closing fifo %s\n", client_pipe_name);
      return -1;
    };

    // close client pipe
    if (close(client_fifo) == -1)
      WARN("Error closing fifo %s\n", client_pipe_name);
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
      WARN("Error while getting file bytes");
      if (close(client_fifo) == -1)
        WARN("Error closing fifo %s\n", client_pipe_name);
      return -1;
    }

    char buffer[MESSAGE_SIZE];
    BOX_SIZE box_size = (BOX_SIZE)tfs_read(fhandle, buffer, 1024);

    if (box_size == -1)
    {
      WARN("Error while getting file bytes");
      if (close(client_fifo) == -1)
        WARN("Error closing fifo %s\n", client_pipe_name);
      return -1;
    }

    // parse wire message
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%hhd|%hhd|%s|%ld|%ld|%ld", RETURN_LIST_BOXES, i == server_state->box_count - 1, name, box_size, pub, sub);

    // write to client pipe
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      WARN("Error while writing to client fifo");
      if (close(client_fifo) == -1)
        WARN("Error closing fifo %s\n", client_pipe_name);
      return -1;
    };
  }

  // close client fifo
  if (close(client_fifo) == -1)
    WARN("Error closing fifo %s\n", client_pipe_name);
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
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s", RETURN_DELETE_BOX, -1, "Box does not exist");

    // write to client pipe
    int client_fifo = open(client_pipe_name, O_WRONLY);
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      if (close(client_fifo) == -1)
        WARN("Error closing fifo %s\n", client_pipe_name);
      return -1;
    }

    // close client pipe
    if (close(client_fifo) == -1)
      WARN("Error closing fifo %s\n", client_pipe_name);

    return 0;
  }

  BoxData *box = server_state->boxes[box_index];

  if (box->subs || box->pubs)
  {
    // build ERROR response
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s", RETURN_DELETE_BOX, -1, "Box is still in use");

    // write to client pipe
    int client_fifo = open(client_pipe_name, O_WRONLY);
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      if (close(client_fifo) == -1)
        WARN("Error closing fifo %s\n", client_pipe_name);
      return -1;
    }

    // close client pipe
    if (close(client_fifo) == -1)
      WARN("Error closing fifo %s\n", client_pipe_name);

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
      WARN("Error while writing to client fifo");
      if (close(client_fifo) == -1)
        WARN("Error closing fifo %s\n", client_pipe_name);
      return -1;
    }

    // close client pipe
    if (close(client_fifo) == -1)
      WARN("Error closing fifo %s\n", client_pipe_name);

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
    WARN("Error while writing to client fifo");
    if (close(client_fifo) == -1)
      WARN("Error closing fifo %s\n", client_pipe_name);
    return -1;
  }

  // close client pipe
  if (close(client_fifo) == -1)
    WARN("Error closing fifo %s\n", client_pipe_name);

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

void thread_function(pc_queue_t *pcq)
{
  while (1)
  {
    // get register message from producer-consumer queue
    char *register_message = pcq_dequeue(pcq);

    // parse register message
    OP_CODE_SIZE op_code;
    char client_pipe_name[PIPE_NAME_SIZE];
    char box_name[BOX_NAME_SIZE];
    sscanf(register_message, "%hhd|%[^|]|%s", &op_code, client_pipe_name, box_name);

    // free memory allocated for register message
    // free(register_message);

    // handle session
    session(op_code, client_pipe_name, box_name);
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
  WARN("Creating file system\n");
  if (tfs_init(NULL) == -1)
  {
    WARN("Error creating file system");
    return -1;
  };

  if (initServerState() == -1)
  {
    WARN("Error initializing server state");
    return -1;
  }

  // init n_sessions threads
  pc_queue_t pcq;

  pcq_create(&pcq, n_sessions + 20);

  for (int i = 0; i < n_sessions; i++)
  {
    pthread_t thread;
    pthread_create(&thread, NULL, (void *)thread_function, &pcq);
  }

  // setup signal handler to handle client CTRL-C
  signal(SIGINT, handleSIGINT);

  // receive register messages
  while (1)
  {
    if (exit_flag)
    {
      break;
    }

    // open the register pipe
    int register_fifo = open(register_pipe_name, O_RDONLY);

    // read from the register pipe
    char buffer[PROTOCOL_MESSAGE_SIZE];
    if (read(register_fifo, buffer, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      WARN("Error while reading register fifo");
      return -1;
    };
    // enqueue register message
    pcq_enqueue(&pcq, buffer);

    close(register_fifo);
  }

  // free file system and queue
  tfs_destroy();
  pcq_destroy(&pcq);

  return 0;
}