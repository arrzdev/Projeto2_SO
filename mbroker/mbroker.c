#include "logging.h"
#include "string.h"
#include "wire_protocol.h"

int main(int argc, char **argv)
{
  if (argc != 3)
  {
    printf("too few arguments");
    return -1;
  }

  char *register_pipe_name = argv[1];
  // char *n_sessions = argv[2];

  // create the register pipe
  if (mkfifo(register_pipe_name, 0666) == -1)
  {
    perror("error creating register pipe");
    return -1;
  }

  printf("Started listening on register fifo\n");
  // open the register pipe
  int register_fifo = open(register_pipe_name, O_RDONLY);
  char last_wire_message[MAX_WIRE_MESSAGE_SIZE];

  while (1)
  {
    // read from the register pipe
    char buffer[MAX_WIRE_MESSAGE_SIZE];
    if (read(register_fifo, buffer, MAX_WIRE_MESSAGE_SIZE) == -1)
    {
      printf("Error while reading register fifo");
      return -1;
    };

    if (strcmp(buffer, last_wire_message))
    {
      // parse wire message
      uint8_t op_code;
      char client_pipe_name[MAX_CLIENT_FIFO_NAME_SIZE];
      char box_name[MAX_BOX_NAME_SIZE];

      sscanf(buffer, "%hhd|%[^|]|%s", &op_code, client_pipe_name, box_name);

      printf("op_code: %hhd\n", op_code);
      printf("fifo_name: %s\n", client_pipe_name);
      printf("box_name: %s\n\n", box_name);

      // update last wire message
      strcpy(last_wire_message, buffer);
    }
  }

  // close the register pipe
  return 0;
}
