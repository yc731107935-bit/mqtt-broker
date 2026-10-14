# -----------------------------------------------------------------------------
# mqtt-broker - standalone MQTT 3.1.1 broker
#
#   make                        release build -> build/release/bin/mqtt_broker
#   make MODE=debug             debug build   -> build/debug/bin/mqtt_broker
#   make CROSS_COMPILE=<prefix> override the toolchain prefix
# -----------------------------------------------------------------------------
CROSS_COMPILE ?= arm-linux-gnueabihf-sigmastar-11.1.0-
CC      := $(CROSS_COMPILE)gcc
STRIP   := $(CROSS_COMPILE)strip
RM      := rm -rf

TARGET  := mqtt_broker
MODE    ?= release
OUT_DIR := build/$(MODE)
OBJ_DIR := $(OUT_DIR)/obj
BIN_DIR := $(OUT_DIR)/bin

# Layer directories
APP_DIR    := src/app
BROKER_DIR := src/broker
NET_DIR    := src/net
UTIL_DIR   := src/util

INCS := $(BROKER_DIR) $(NET_DIR) $(UTIL_DIR)

ARCH_FLAGS := $(EXTERN_PLAT_CFLAGS)
WARN_FLAGS := -Wall -Wextra -Wno-unused-parameter
CFLAGS := $(ARCH_FLAGS) $(WARN_FLAGS) -fPIC -MMD -MP
CFLAGS += $(addprefix -I,$(INCS))

ifeq ($(MODE),debug)
    CFLAGS += -O0 -g -ggdb -DDEBUG
else
    CFLAGS += -O2 -DNDEBUG
endif

LDFLAGS := $(EXTERN_PLAT_LDFLAGS)
LIBS    := -lpthread -lm

SRCS := $(APP_DIR)/main.c \
        $(BROKER_DIR)/mqtt_broker.c \
        $(BROKER_DIR)/message_queue.c \
        $(NET_DIR)/event_loop.c \
        $(UTIL_DIR)/log.c \
        $(UTIL_DIR)/time_utils.c

OBJS := $(SRCS:%.c=$(OBJ_DIR)/%.o)
DEPS := $(OBJS:.o=.d)

.PHONY: all clean test docs lint

all: $(BIN_DIR)/$(TARGET)
	@echo "[ok] $(BIN_DIR)/$(TARGET)"

$(BIN_DIR)/$(TARGET): $(OBJS)
	@mkdir -p $(dir $@)
	$(CC) $^ -o $@ $(LDFLAGS) $(LIBS)
	@if [ "$(MODE)" = "release" ]; then $(STRIP) $@; fi

$(OBJ_DIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	$(RM) $(OUT_DIR) release

-include $(DEPS)

# -----------------------------------------------------------------------------
# Unit tests (always built with the host compiler)
# -----------------------------------------------------------------------------
HOSTCC  ?= gcc
TEST_DIR := tests

# Tool overrides (documentation / static analysis)
DOXYGEN ?= doxygen
CPPCHECK ?= cppcheck

test: $(OUT_DIR)/test_message_queue
	./$(OUT_DIR)/test_message_queue

$(OUT_DIR)/test_message_queue: $(TEST_DIR)/test_message_queue.c $(BROKER_DIR)/message_queue.c $(BROKER_DIR)/message_queue.h
	@mkdir -p $(dir $@)
	$(HOSTCC) -std=c11 -Wall -Wextra -Werror -I$(BROKER_DIR) -I$(TEST_DIR) \
	        $(TEST_DIR)/test_message_queue.c $(BROKER_DIR)/message_queue.c -o $@

# -----------------------------------------------------------------------------
# Documentation (Doxygen, HTML output in docs/generated/html)
# -----------------------------------------------------------------------------
docs:
	$(DOXYGEN) Doxyfile
	@echo "[ok] docs/generated/html/index.html"

# -----------------------------------------------------------------------------
# Static analysis (cppcheck)
# -----------------------------------------------------------------------------
lint:
	$(CPPCHECK) --std=c11 --enable=warning,style,performance,portability \
	        --inline-suppr --error-exitcode=2 --suppress=missingIncludeSystem \
	        -I$(BROKER_DIR) -I$(NET_DIR) -I$(UTIL_DIR) src tests
