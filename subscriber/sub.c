#include "logging.h"
#include "client.h"
#include "wire_protocol.h"
#include "string.h"

int main(int argc, char **argv)
{
  if (argc != 4)
  {
    WARN("number of arguments invalid");
    return -1;
  }

  char *register_pipe_name = argv[1];
  char *client_pipe_name = argv[2];
  char *box_name = argv[3];

  // connect to server
  if (connect(REGISTER_SUBSCRIBER, register_pipe_name, client_pipe_name, box_name) == -1)
  {
    WARN("error connecting to server");
    return -1;
  }

  // open the client fifo
  int client_fifo = open(client_pipe_name, O_RDONLY);

  char buffer[PROTOCOL_MESSAGE_SIZE];
  
  while(1){
    if(read(client_fifo, buffer, PROTOCOL_MESSAGE_SIZE) == 0)
      break;
    printf("Received: %s\n", buffer);
  }

  // close fifo
  close(client_fifo);

  // unlink fifo
  unlink(client_pipe_name);
  return 0;
}