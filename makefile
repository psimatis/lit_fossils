# Detect operating system
OS := $(shell uname)
ifeq ($(OS),Darwin)
	CC      = /opt/homebrew/bin/g++-13
	CFLAGS  = -O3 -std=c++14 -w -march=native -I/opt/homebrew/opt/boost/include -I/opt/homebrew/include/spatialindex
	LDFLAGS = -L/opt/homebrew/lib -Wl,-rpath,/opt/homebrew/lib -lspatialindex
else
	CC      = g++
	CFLAGS  = -O3 -mavx -std=c++14 -w -I/usr/include/spatialindex
	LDFLAGS = -L/usr/lib -Wl,-rpath=/usr/lib -lspatialindex
endif

# Source files
SOURCES = utils.cpp containers/relation.cpp containers/offsets_templates.cpp containers/offsets.cpp containers/buffer.cpp indices/hierarchicalindex.cpp indices/hint_m_dynamic_naive.cpp indices/hint_m_dynamic_sec_attr.cpp indices/hint_m_dynamic.cpp indices/live_index.cpp indices/fossil_index.cpp
OBJECTS = $(SOURCES:.cpp=.o)

# Targets
all: query

query: pureLIT teHINT aLIT fossilLIT

# Executable for pureLIT
pureLIT: $(OBJECTS)
	$(CC) $(CFLAGS) utils.o containers/relation.o containers/buffer.o indices/hierarchicalindex.o indices/live_index.o indices/hint_m_dynamic.o main_pureLIT.cpp -o query_pureLIT.exec $(LDFLAGS)

# Executable for teHINT
teHINT: $(OBJECTS)
	$(CC) $(CFLAGS) utils.o containers/relation.o indices/hierarchicalindex.o indices/hint_m_dynamic_naive.o main_teHINT.cpp -o query_teHINT.exec $(LDFLAGS)

# Executable for aLIT
aLIT: $(OBJECTS)
	$(CC) $(CFLAGS) utils.o containers/relation.o containers/buffer.o indices/hierarchicalindex.o indices/live_index.o indices/hint_m_dynamic_sec_attr.o main_aLIT.cpp -o query_aLIT.exec $(LDFLAGS)

# Executable for fossilLIT
fossilLIT: $(OBJECTS)
	$(CC) $(CFLAGS) utils.o containers/relation.o containers/buffer.o indices/hierarchicalindex.o indices/live_index.o indices/hint_m_dynamic.o indices/fossil_index.o main_fossilLIT.cpp -o query_fossilLIT.exec $(LDFLAGS)

# Rule for compiling .cpp files to .o files
.cpp.o:
	$(CC) $(CFLAGS) -c $< -o $@

# Clean target to remove object files and executables
clean:
	rm -rf utils.o
	rm -rf containers/*.o
	rm -rf indices/*.o
	rm -rf query_teHINT.exec
	rm -rf query_pureLIT.exec
	rm -rf query_aLIT.exec
	rm -rf query_fossilLIT.exec
