CXX=g++
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
LDFLAGS= -lpthread
else
LDFLAGS= -lpthread -lrt -lprotobuf
endif
ifdef RONDIS_DEBUG
CXXFLAGS=-g -std=c++17 -fno-builtin-memcmp
else
CXXFLAGS=-O2 -std=c++17 -fno-builtin-memcmp
endif
ifeq ($(shell uname -m), x86_64)
#    CXXFLAGS += -msse -msse4.2
endif

.PHONY: clean all

all: rondis

ifndef RONDB_PATH
  $(warning Warning: missing rondb path)
  $(error Exit due to missing RONDB_PATH)
endif
RONDB_INCLUDE_DIR=$(RONDB_PATH)/include/storage/ndb
ifeq ($(UNAME_S),Darwin)
RONDB_LIBRARY=$(RONDB_PATH)/lib/libndbclient.dylib
else
RONDB_LIBRARY=$(RONDB_PATH)/lib/libndbclient.so
endif

CXXFLAGS+= -I$(CURDIR)/../include -I$(CURDIR) -I$(RONDB_INCLUDE_DIR)

DEP_LIBS = $(RONDB_LIBRARY)
ifeq ($(UNAME_S),Darwin)
DEP_LIBS+= /opt/homebrew/lib/libprotobuf.dylib
endif
LDFLAGS := $(DEP_LIBS) $(LDFLAGS)

# Use find to locate all .cc files in subdirectories
SOURCES = $(CURDIR)/rondis.cc $(CURDIR)/rondb.cc $(CURDIR)/common.cc $(CURDIR)/table_definitions.cc $(CURDIR)/commands.cc $(CURDIR)/db_operations.cc $(CURDIR)/interpreted_code.cc $(CURDIR)/dispatch_thread.cc $(CURDIR)/pink_conn.cc $(CURDIR)/pink_epoll.cc $(CURDIR)/pink_thread.cc $(CURDIR)/pink_util.cc $(CURDIR)/redis_conn.cc $(CURDIR)/redis_parser.cc $(CURDIR)/server_socket.cc $(CURDIR)/server_thread.cc $(CURDIR)/worker_thread.cc
OBJECTS = $(SOURCES:.cc=.o)

# Target to build the executable "rondis"
rondis: $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJECTS) $(LDFLAGS)

# Clean target to remove generated files
clean:
	rm -f $(OBJECTS) rondis
