#include "logging.h"
#include "client.h"
#include "wire_protocol.h"
#include "signal.h"
#include "errno.h"

volatile sig_atomic_t disconnect_flag = 0;

static void handleSIGINT(int sig)
{
  (void)sig;
  disconnect_flag = 1;
}

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
    printf("error connecting to server");
    return -1;
  }

  //check if server unlinked client fifo meaning something could be processed

  // sleep for 1s to allow server to unlink client fifo
  sleep(1);

  if (access(client_pipe_name, F_OK) != 0)
  {
    printf("Error occured\n");
    return -1;
  }

  // open the client fifo
  int client_fifo = open(client_pipe_name, O_RDONLY);

  // setup signal handler to handle client CTRL-C
  signal(SIGINT, handleSIGINT);

  char buffer[PROTOCOL_MESSAGE_SIZE];
  char message[MESSAGE_SIZE];
  int message_count = 0;

  while (1)
  {
    ssize_t bytes_read = read(client_fifo, buffer, PROTOCOL_MESSAGE_SIZE);

    if (bytes_read <= 0)
      break;

    if (disconnect_flag)
      break;

    // parse message in form of "op_code|message" and ignore op_code
    sscanf(buffer, "%*d|%[^\n]", message);

    // print message
    fprintf(stdout, "%s\n", message);

    // increment message count
    message_count++;
  }

  fprintf(stdout, "%d\n", message_count);

  printf("Disconnected...\n");

  // close fifo
  if (close(client_fifo) == -1)
    WARN("Error closing fifo %s\n", client_pipe_name);

  // unlink fifo
  unlink(client_pipe_name);
  return 0;
}