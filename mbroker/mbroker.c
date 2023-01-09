#include "logging.h"
#include "string.h"
#include "wire_protocol.h"

#include "operations.h"
#include "state.h"

void session(OP_CODE_SIZE op_code, char *client_pipe_name, char *box_name)
{
  (void)box_name;

  switch (op_code)
  {
  case REGISTER_PUBLISHER:
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
        return;
      };

      // parse wire message
      OP_CODE_SIZE message_op_code;
      char message[MESSAGE_SIZE];
      sscanf(buffer, "%hhd|%[^\n]", &message_op_code, message);

      printf("Publisher: %s\n", message);

      // TODO: write the message in box located in tfs
    }

  case CREATE_BOX:
    char wire_message[PROTOCOL_MESSAGE_SIZE];
    bool error = false;
    
    // TODO check if box exists

    // create box
    int fhandle = tfs_open(box_name, TFS_O_CREAT);

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

    // open client pipe
    if (access(client_pipe_name, F_OK))
    {
      printf("[Manager disconnected]\n");
      break;
    }

    int client_fifo = open(client_pipe_name, O_WRONLY);

    // write to client pipe
    if (write(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error while writing to client fifo");
      return;
    };

    if(error) printf("[Box created]\n");
    else printf("[Box creation failed]\n");

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

    // this a single threaded version of broker so here we just keep attending to the client that just registered
    session(op_code, client_pipe_name, box_name);

    close(register_fifo);
  }

  return 0;
}