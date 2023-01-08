// read/write lib
#include <unistd.h>

// fifos lib
#include <sys/stat.h>

// O_RDONLY and O_WRONLY lib
#include <fcntl.h>

// uint_8 lib
#include <stdint.h>

#define MAX_WIRE_MESSAGE_SIZE 291
#define MAX_BOX_NAME_SIZE 32
#define MAX_CLIENT_FIFO_NAME_SIZE 256