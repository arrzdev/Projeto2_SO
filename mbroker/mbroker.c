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

  // handle publisher life cycle

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
  printf("[Publisher disconnected]\n");
  // close client fifo
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
  server_state->boxes = realloc(server_state->boxes, sizeof(BoxData *) * (server_state->box_count + 1));
  if (server_state->boxes == NULL)
    return -1;
  server_state->boxes[server_state->box_count] = box;
  server_state->box_count++;

  return 0;
}

int listBoxes(char *client_pipe_name)
{
  char wire_message[PROTOCOL_MESSAGE_SIZE];

  // TODO: check if client exists before trying to send the list

  // open client pipe
  int client_fifo = open(client_pipe_name, O_WRONLY);

  // check if no box was created
  if (server_state->box_count == 0)
  {
    // write message
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s|%d|%d|%d", RETURN_LIST_BOXES, 1, "\0", 0, 0, 0);

    // write to client pipe
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while writing to client fifo");
      return -1;
    };

    return 0;
  }

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
      return -1;
    }

    char buffer[MESSAGE_SIZE];
    BOX_SIZE box_size = (BOX_SIZE)tfs_read(fhandle, buffer, 1024);

    if (box_size == -1)
    {
      printf("Error while getting file bytes");
      return -1;
    }

    // write message
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%hhd|%hhd|%s|%ld|%ld|%ld", RETURN_LIST_BOXES, i == server_state->box_count - 1, name, box_size, pub, sub);

    // write to client pipe
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while writing to client fifo");
      return -1;
    };
  }

  return 0;
}

int deleteBox(char *client_pipe_name, char *box_name)
{
  // find the box position in server state list
  int deleted = 0;
  char error_message[MESSAGE_SIZE];
  for (int i = 0; i <= server_state->box_count; i++)
  {
    BoxData *current_box_data = server_state->boxes[i];
    char *current_box_name = current_box_data->name;
    // if the box is not the one we want we keep looking forward
    if (strcmp(current_box_name, box_name) != 0)
      continue;

    // format string for tfs
    char box_name_update[BOX_NAME_SIZE + 1] = "/";
    strcat(box_name_update, box_name);

    // create response message
    if (tfs_unlink(box_name_update) == -1)
    {
      strcpy(error_message, "Error deleting box file from tfs");
      break;
    }

    // free boxdata info
    free(current_box_name);
    free(current_box_data);

    // if we have more than 2 box's and the box isn't the last one we swap it with the last one
    if (i != 1 && i != server_state->box_count)
    {
      server_state->boxes[i] = server_state->boxes[server_state->box_count];
    }

    // realloc the memory to get rid of the last box
    server_state->boxes = realloc(server_state->boxes, sizeof(BoxData *) * (server_state->box_count - 1));
    if (server_state->boxes == NULL)
      return -1;

    server_state->box_count--;
    deleted = 1;
    break;
  }

  char wire_message[PROTOCOL_MESSAGE_SIZE];
  if (deleted)
  {
    // success reponse
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d | %d | %s", RETURN_DELETE_BOX, 0, "\0");
  }
  else
  {
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d | %d | %s", RETURN_DELETE_BOX, -1, error_message);
  }

  // TODO: check if client still exists before trying to return the response
  // open client pipe
  int client_fifo = open(client_pipe_name, O_WRONLY);

  // send response to client
  if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
  {
    printf("Error while sending action response to manager\n");
    return -1;
  };

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