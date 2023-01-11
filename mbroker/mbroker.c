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
  long unsigned int boxCount;
} State;

State *serverState;

// init global state
int initServerState()
{
  serverState = (State *)malloc(sizeof(State));

  if (serverState == NULL)
    return -1;

  serverState->boxCount = 0;
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
  printf("[Publisher connected]\n");

  while (1)
  {
    // check if the client is still connected
    if (access(client_pipe_name, F_OK))
    {
      printf("[Publisher disconnected]\n");
      break;
    }

    // connect to publisher pipe
    int client_fifo = open(client_pipe_name, O_RDONLY);

    // read from client fifo
    char buffer[PROTOCOL_MESSAGE_SIZE];
    if (read(client_fifo, buffer, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while reading client fifo");
      return -1;
    };

    // close client fifo
    close(client_fifo);

    // parse wire message
    OP_CODE_SIZE message_op_code;
    char message[MESSAGE_SIZE];
    sscanf(buffer, "%hhd|%[^\n]", &message_op_code, message);

    printf("Publisher: %s\n", message);

    //  TODO: write the message in box located in tfs
  }

  return 0;
}

int createBox(char *client_pipe_name, char *box_name)
{
  char wire_message[PROTOCOL_MESSAGE_SIZE];
  bool error = false;

  // format string for tfs
  char box_name_update[BOX_NAME_SIZE + 1] = "/";

  strcat(box_name_update, box_name);

  // create box in tfs open
  int fhandle = tfs_open(box_name_update, TFS_O_CREAT);

  if (fhandle == -1 || tfs_close(fhandle) == -1)
  {
    // send response to client
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s", RETURN_CREATE_BOX, -1, "Error creating box");
    error = true;
  }
  else
  {
    // send response to client
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d|%d|%s", RETURN_CREATE_BOX, 0, "\0");
  }

  // check if client pipe exists
  if (access(client_pipe_name, F_OK))
  {
    // TODO: when manager is not alive to receive the reponse it's supposed to still process the actions?
    printf("[Manager disconnected]\n");
    return 0;
  }

  // open client pipe
  int client_fifo = open(client_pipe_name, O_WRONLY);

  // write to client pipe
  if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
  {
    printf("Error while writing to client fifo");
    return -1;
  };

  if (error)
    printf("[Box creation failed]\n");
  else
    printf("[Box created]\n");

  // add box to the list of boxes
  serverState->boxes = realloc(serverState->boxes, sizeof(BoxData *) * (serverState->boxCount + 1));
  if (serverState->boxes == NULL)
    return -1;

  BoxData *box = (BoxData *)malloc(sizeof(BoxData));

  if (box == NULL)
    return -1;

  box->name = (char *)malloc(sizeof(char) * (strlen(box_name) + 1));

  if (box->name == NULL)
    return -1;

  strcpy(box->name, box_name);

  box->subs = 0;
  box->pubs = 0;

  serverState->boxes[serverState->boxCount] = box;

  serverState->boxCount++;

  return 0;
}

int listBoxes(char *client_pipe_name)
{
  char wire_message[PROTOCOL_MESSAGE_SIZE];

  // check if client pipe exists
  if (access(client_pipe_name, F_OK))
  {
    // TODO: when manager is not alive to receive the reponse it's supposed to still process the actions?
    printf("[Manager disconnected]\n");
    return 0;
  }

  // open client pipe
  int client_fifo = open(client_pipe_name, O_WRONLY);

  // check if no box was created
  if (serverState->boxCount == 0)
  {
    // write message
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d | %d | %s | %d | %d | %d", RETURN_LIST_BOXES, 1, "\0", 0, 0, 0);

    // write to client pipe
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while writing to client fifo");
      return -1;
    };

    return 0;
  }

  for (int i = 0; i <= serverState->boxCount; i++)
  {

    char *name = serverState->boxes[i]->name;
    uint64_t pub = serverState->boxes[i]->pubs;
    uint64_t sub = serverState->boxes[i]->subs;

    // format string for tfs
    char box_name_update[BOX_NAME_SIZE + 1] = "/";

    strcat(box_name_update, name);

    int fhandle = tfs_open(box_name_update, 0);

    if (fhandle == -1)
    {
      printf("Error while getting file bytes");
      return -1;
    }

    char buffer[1024];
    int size = (int)tfs_read(fhandle, buffer, 1024);

    if (size == -1)
    {
      printf("Error while getting file bytes");
      return -1;
    }

    // write message
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d | %d | %s | %d | %ld | %ld", RETURN_LIST_BOXES, i == serverState->boxCount, name, size, pub, sub);

    // write to client pipe
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while writing to client fifo");
      return -1;
    };
  }

  return 0;
}

int deleteBox(char *client_pipe_name, char *boxName)
{
  for (int i = 0; i <= serverState->boxCount; i++)
  {
    char *name = serverState->boxes[i]->name;
    if (strcmp(boxName, name) != 0)
      continue;

    BoxData *tmp = serverState->boxes[i];

    // format string for tfs
    char box_name_update[BOX_NAME_SIZE + 1] = "/";

    strcat(box_name_update, boxName);

    char wire_message[PROTOCOL_MESSAGE_SIZE];

    // delete success
    if (tfs_unlink(box_name_update) == -1)
    {
      snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d | %d | %s", RETURN_DELETE_BOX, -1, "Error deleting box");
    }
    // delete failure
    else
    {
      snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d | %d | %s", RETURN_DELETE_BOX, 0, "\0");
    }

    // check if client pipe exists
    if (access(client_pipe_name, F_OK))
    {
      // TODO: when manager is not alive to receive the reponse it's supposed to still process the actions?
      printf("[Manager disconnected]\n");
      return 0;
    }

    // open client pipe
    int client_fifo = open(client_pipe_name, O_WRONLY);

    // write message
    snprintf(wire_message, PROTOCOL_MESSAGE_SIZE, "%d | %d | %s | %d | %d | %d", RETURN_LIST_BOXES, 1, "\0", 0, 0, 0);

    // write to client pipe
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while writing to client fifo");
      return -1;
    };

    // free boxdata info
    free(name);
    free(tmp);

    if (i != serverState->boxCount)
    {
      serverState->boxes[i] = serverState->boxes[serverState->boxCount];
    }

    serverState->boxes = realloc(serverState->boxes, sizeof(BoxData *) * (serverState->boxCount - 1));
    if (serverState->boxes == NULL)
      return -1;

    serverState->boxCount--;
  }

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

    // parse wire message
    OP_CODE_SIZE op_code;
    char client_pipe_name[PIPE_NAME_SIZE];
    char box_name[BOX_NAME_SIZE];
    sscanf(buffer, "%hhd|%[^|]|%s", &op_code, client_pipe_name, box_name);

    printf("%s\n", client_pipe_name);
    printf("%s\n", box_name);

    // this a single threaded version of broker so here we just keep attending to the client that just registered
    session(op_code, client_pipe_name, box_name);

    close(register_fifo);
  }

  return 0;
}