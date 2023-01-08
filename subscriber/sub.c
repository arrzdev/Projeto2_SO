#include "logging.h"
#include "wire_protocol.h"

#define OP_CODE 2

int main(int argc, char **argv)
{
  if (argc != 4)
  {
    WARN("number of arguments invalid");
    return -1;
  }

  // char *register_pipe_name = argv[1];
  char *client_pipe_name = argv[2];
  char *box_name = argv[3];

  char wire_message[MAX_WIRE_MESSAGE_SIZE];
  snprintf(wire_message, MAX_WIRE_MESSAGE_SIZE, "%d|%s|%s", OP_CODE, client_pipe_name, box_name);

  printf("Subscriber Sent: %s\n", wire_message);

  return 0;
}
