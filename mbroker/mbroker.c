#include "logging.h"
#include "string.h"
#include "wire_protocol.h"

void session(OP_CODE_SIZE op_code, char *client_pipe_name, char *box_name)
{
  (void)box_name;

  switch (op_code)
  {
  case REGISTER_PUBLISHER:
    // handle publisher life cycle
    printf("Publisher connected\n");

    while (1)
    {
      // connect to publisher pipe
      int client_fifo = open(client_pipe_name, O_RDONLY);
      if (client_fifo == -1) // TODO: this doesnt work
      {
        printf("client doesnt exist");
      }

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