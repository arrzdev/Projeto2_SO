#include "logging.h"
#include "wire_protocol.h"
#include "utils/client.h"

static void print_usage()
{
  fprintf(stderr, "usage: \n"
                  "   manager <register_pipe_name> <pipe_name> create <box_name>\n"
                  "   manager <register_pipe_name> <pipe_name> remove <box_name>\n"
                  "   manager <register_pipe_name> <pipe_name> list\n");
}

int createRemoveBox(OP_CODE_SIZE action_code, char *register_pipe_name, char *client_pipe_name, char *box_name)
{
  // send create request by connecting to server with CREATE_BOX op_code
  if (connect(action_code, register_pipe_name, client_pipe_name, box_name) == -1)
  {
    printf("Error connecting to server");
    return -1;
  }

  OP_CODE_SIZE op_code;
  RETURN_CODE_SIZE return_code;
  char error_message[MESSAGE_SIZE];

  // open client_fifo to receive server response
  int client_fifo = open(client_pipe_name, O_RDONLY);

  // listen for response
  char wire_message[PROTOCOL_MESSAGE_SIZE];
  if (read(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
  {
    printf("Error reading response from server");
    return -1;
  }

  sscanf(wire_message, "%hhd|%d|%[^\n]", &op_code, &return_code, error_message);

  // check if the message is directed to us (should be but..)
  if (op_code != RETURN_CREATE_BOX)
  {
    printf("Received misplaced response from server");
    return -1;
  }

  // print response
  if (return_code == 0)
    fprintf(stdout, "OK\n");
  else
    fprintf(stdout, "ERROR %s\n", error_message);

  // close client fifo
  close(client_fifo);

  // unlink client fifo
  unlink(client_pipe_name);

  return 0;
}

int listBox(char *register_pipe_name, char *client_pipe_name)
{
  if (connect(LIST_BOXES, register_pipe_name, client_pipe_name, "\0") == -1)
  {
    printf("Error connecting to server");
    return -1;
  }

  OP_CODE_SIZE op_code;
  uint8_t last_message;
  char box_name[32];
  uint64_t box_size;
  uint64_t n_publishers;
  uint64_t n_subscribers;

  int client_fifo = open(client_pipe_name, O_RDONLY);

  // listen for response
  char wire_message[PROTOCOL_MESSAGE_SIZE];
  while (!last_message)
  {
    if (read(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error reading response from server");
      return -1;
    }

    // parse
    sscanf(wire_message, "%hhd|%hhd|%[^|]|%lu|%lu|%lu", &op_code, &last_message, box_name, &box_size, &n_publishers, &n_subscribers);

    if (op_code != RETURN_LIST_BOXES) // check if the message is directed to us (should be but..)
      continue;

    if (strlen(box_name) != 0)
      fprintf(stdout, "%s %zu %zu %zu\n", box_name, box_size, n_publishers, n_subscribers);
    else
    {
      fprintf(stdout, "NO BOXES FOUND\n");
    }
  }

  // close client fifo
  close(client_fifo);

  // unlink client fifo
  unlink(client_pipe_name);

  return 0;
}

int main(int argc, char **argv)
{
  if (argc < 4)
  {
    printf("too few arguments\n");
    return -1;
  }

  char *register_pipe_name = argv[1];
  char *command = argv[3];

  if (!strcmp(command, "create") || !strcmp(command, "remove"))
  {
    if (argc != 5)
    {
      printf("too few arguments\n");
      return -1;
    }

    // parsing
    char *pipe_name = argv[2];
    char *box_name = argv[4];

    // create or delete box
    OP_CODE_SIZE action_op_code;
    if (!strcmp(command, "create"))
      action_op_code = CREATE_BOX;
    else
      action_op_code = DELETE_BOX;

    return createRemoveBox(action_op_code, register_pipe_name, pipe_name, box_name);
  }
  else if (strcmp(command, "list") == 0)
  {
    if (argc != 4)
    {
      printf("too few arguments\n");
      return -1;
    }

    char *pipe_name = argv[2];

    return listBox(register_pipe_name, pipe_name);
  }
  else
  {
    print_usage();
    return -1;
  }

  return 0;
}
