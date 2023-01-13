#include "logging.h"
#include "string.h"
#include "wire_protocol.h"

#include "operations.h"
#include "state.h"

typedef struct
{
  char *name;
  uint64_t subs;
  uint64_t pubs;
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

  box->subs = 0;
  box->pubs = 0;

  return box;
}

int handlePublisher(char *client_pipe_name, char *box_name)
{
  (void)box_name;

  // TODO: only 1 publisher at a time is supposed to be connected to the box, so if a publisher tries to connect to a box and a publisher is already connected what happen?

  // TODO: if a publisher or a subscriber try to connect to a box that doesnt exist, what happen?

  // connect to publisher
  int client_fifo = open(client_pipe_name, O_RDONLY);
  printf("[Publisher connected]\n");

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

    printf("Publisher: %s\n", message);
    //  TODO: write the message in box located in tfs
  }

  // close fifo
  close(client_fifo);

  // TODO: when publisher disconnects from the box, we need to decrement the n_publishers variable in the box_data
  printf("[Publisher disconnected]\n");
  return 0;
}

int createBox(char *client_pipe_name, char *box_name)
{
  char wire_message[PROTOCOL_MESSAGE_SIZE];

  // format string for tfs
  char box_name_update[BOX_NAME_SIZE + 1] = "/";

  // TODO: check if box already exist (idk how since we dont have access to tfs_lookup)
  // probably need to extend tfs api
  strcat(box_name_update, box_name);

  // create box in tfs open
  int fhandle = tfs_open(box_name_update, TFS_O_CREAT);

  if (fhandle == -1 || tfs_close(fhandle) == -1)
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

  // after creating list boxes for debugging proposes
  for (int i = 0; i < server_state->box_count; i++)
  {
    printf("Box name: %s\n", server_state->boxes[i]->name);
  }

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

  printf("Listing %ld boxes", server_state->box_count);

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

int main(int argc, char **argv)
{
  if (argc != 3)
  {
    printf("too few arguments");
    return -1;
  }

  char *register_pipe_name = argv[1];
  // char *n_sessions = argv[2]; //used when multi-threading

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

    // TODO: add auxiliar functions to build and parse the protocol messages
    //  parse wire message
    OP_CODE_SIZE op_code;
    char client_pipe_name[PIPE_NAME_SIZE];
    char box_name[BOX_NAME_SIZE];
    sscanf(buffer, "%hhd|%[^|]|%s", &op_code, client_pipe_name, box_name);

    // this a single threaded version of broker so here we just keep attending to the client that just registered
    session(op_code, client_pipe_name, box_name);

    close(register_fifo);
  }

  return 0;
}