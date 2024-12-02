OPT?=3
SANITIZE?=0
CILK?=0
DEBUG?=0
AVX512?=0

CXX=clang++
#CXX=g++-11
#CXX=../OpenCilk-10.0.1-Linux/bin/clang++

CFLAGS := -Wall -Wno-address-of-packed-member -Wextra -O$(OPT) -gdwarf-4  -std=c++20 -ftemplate-backtrace-limit=0 -ferror-limit=1 -IParallelTools/


LDFLAGS := -lrt -lm -lpthread -lm -ldl -latomic -ljemalloc

ifeq ($(DEBUG),0)
CFLAGS += -DNDEBUG
endif
ifeq ($(CILK),1)
CXX=clang++
CFLAGS += -fcilkplus -DCILK=1
LDFLAGS += -L/home/ubuntu/xvdf_mounted/cilkrts/build/install/lib
endif
ifeq ($(SANITIZE),1)
CFLAGS += -fsanitize=undefined,address -fno-omit-frame-pointer
endif

ifeq ($(OPT),3)
CFLAGS += -fno-signed-zeros  -freciprocal-math -ffp-contract=fast -fno-trapping-math  -ffinite-math-only 
endif

ifeq ($(AVX512),1)
CFLAGS += -march=native
endif

DEFINES := -DCILK=$(CILK) -DDEBUG=$(DEBUG) -DAVX512=$(AVX512)

VERIFY_COUNT ?= 1000


INCLUDES := leafDS.hpp 
.PHONY: all clean tidy

all:  basic test 
#build_profile profile opt


basic: test.cpp leafDS.hpp StructOfArrays/SizedInt.hpp StructOfArrays/soa.hpp
	$(CXX) $(CFLAGS) $(DEFINES) test.cpp $(LDFLAGS) -o basic


test: basic
	@mkdir -p test_out
	@./basic --update_test --el_count $(VERIFY_COUNT) --verify  >test_out/update_test || (echo "./basic --update_test --el_count $(VERIFY_COUNT) --verify verification failed $$?"; exit 1)&
	@./basic --update_values_test --el_count $(VERIFY_COUNT) --verify  >test_out/update_values_test || (echo "./basic --update_values_test --el_count $(VERIFY_COUNT) --verify verification failed $$?"; exit 1)&
	@wait
	@sleep 1
	@echo "Tests Finished"


clean:
	rm -f basic
