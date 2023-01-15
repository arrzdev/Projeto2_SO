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

// compare function to feed to qsort to compare box string representations by box_name
int compareBoxes(const void *a, const void *b)
{
  char *box_a = *(char **)a;
  char *box_b = *(char **)b;

  char *box_a_name = strtok(box_a, "|");
  char *box_b_name = strtok(box_b, "|");

  return strcmp(box_a_name, box_b_name);
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
  if (op_code != RETURN_CREATE_BOX && op_code != RETURN_DELETE_BOX)
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
  if (close(client_fifo) == -1)
WARN("Error closing fifo %s\n", client_pipe_name);

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
  char box_name[BOX_NAME_SIZE];
  uint64_t box_size;
  uint64_t n_publishers;
  uint64_t n_subscribers;

  int client_fifo = open(client_pipe_name, O_RDONLY);

  // listen for response
  char wire_message[PROTOCOL_MESSAGE_SIZE];
  char **boxes = (char **)malloc(sizeof(char *));
  int n_boxes = 0;
  char buffer[PROTOCOL_MESSAGE_SIZE];
  while (!last_message)
  {
    if (read(client_fifo, wire_message, PROTOCOL_MESSAGE_SIZE) == -1)
    {
      printf("Error reading response from server");
      return -1;
    }

    // parse
    if (sscanf(wire_message, "%hhd|%hhd|%[^|]|%lu|%lu|%lu", &op_code, &last_message, box_name, &box_size, &n_publishers, &n_subscribers) < 3)
    {
      break; // box_name is \0
    }

    // check if the message is directed to us (should be but..)
    if (op_code != RETURN_LIST_BOXES)
      break;

    // add string representation of the box to the buffer
    sprintf(buffer, "%s %zu %zu %zu", box_name, box_size, n_publishers, n_subscribers);

    // allocate memory for the buffer that contains the string representation of the box
    boxes[n_boxes] = (char *)malloc((strlen(buffer) + 1) * sizeof(char));

    strcpy(boxes[n_boxes], buffer);

    // increment the dynamic vector size
    n_boxes++;

    // reallocate dynamic vector memory
    boxes = (char **)realloc(boxes, (long unsigned int)n_boxes * sizeof(char *));
  }

  // handle the list of boxes
  if (n_boxes != 0)
  {
    // sort the vector
    qsort(boxes, (long unsigned int)n_boxes, sizeof(char *), compareBoxes);

    // print them
    for (int i = 0; i < n_boxes; i++)
    {
      fprintf(stdout, "%s\n", boxes[i]);
      free(boxes[i]);
    }

    free(boxes);
  }
  else
  {
    fprintf(stdout, "NO BOXES FOUND\n");
  }

  // close client fifo
  if (close(client_fifo) == -1)
WARN("Error closing fifo %s\n", client_pipe_name);

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
