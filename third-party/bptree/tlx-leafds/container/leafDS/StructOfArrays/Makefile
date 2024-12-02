OPT?=fast
VALGRIND?=0
INFO?=0
ENABLE_TRACE_TIMER?=0
CYCLE_TIMER?=0
DEBUG?=0
CILK?=0
SANITIZE?=0
GDB?=0



TAPIR_DIR=../OpenCilk-9.0.1-Linux/

CFLAGS := -Wall -Wextra -O$(OPT) -g  -std=c++20

ifeq ($(SANITIZE),1)
ifeq ($(CILK),1)
CFLAGS += -fsanitize=cilk -fno-omit-frame-pointer
# CFLAGS += -fsanitize=undefined,address -fno-omit-frame-pointer
else
CFLAGS += -fsanitize=undefined,address -fno-omit-frame-pointer
endif
endif

LDFLAGS := -lrt -lm -lpthread -lm -ldl -latomic
# -ljemalloc

ifeq ($(VALGRIND),0)
CFLAGS += -march=native
endif

DEFINES := -DENABLE_TRACE_TIMER=$(ENABLE_TRACE_TIMER) -DCYCLE_TIMER=$(CYCLE_TIMER) -DCILK=$(CILK) -DDEBUG=$(DEBUG)

ifeq ($(CILK),1)
CFLAGS += -fopencilk -DPARLAY_CILK
ONE_WORKER = CILK_NWORKERS=1
endif


ifeq ($(DEBUG),0)
CFLAGS += -DNDEBUG
endif


ifeq ($(INFO), 1) 
CFLAGS +=  -Rpass-missed="(inline|loop*)" -Rpass-analysis="(inline|loop*)" 
# CFLAGS += -Rpass="(inline|loop*)" -Rpass-missed="(inline|loop*)" -Rpass-analysis="(inline|loop*)" 
# CFLAGS += -Rpass=.* -Rpass-missed=.* -Rpass-analysis=.* 
endif


all: basic
 
basic: main.cpp soa.hpp aos.hpp SizedInt.hpp
	$(CXX) $(CFLAGS) $(DEFINES) $(LDFLAGS) -o $@ main.cpp



clean:
	rm -f *.o opt profile basic code.profdata default.profraw cachegrind.out.* perf.data perf.data.old

