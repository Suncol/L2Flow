DEMOS:=$(basename $(wildcard *.cpp))
OBJS:=$(subst .cpp,.o,$(wildcard *.cpp))
LIBDIR:=-L../libs/linux
SODIR:=-Wl,-rpath=./ 
CPPFLAGS:=-g -O2 -std=c++11
INCS:=-I../include -I../3rd
LIBS:=-lmdl_api -ldl -lpthread -lrt -ljson
OUTDIR:=../bin

all: $(DEMOS)

%.o: %.cpp
	g++-4.8 $(INCS) $(CPPFLAGS) -c "$<" -o "$(OUTDIR)/$@"

$(DEMOS): % : %.o
	g++-4.8 $(SODIR) $(OUTDIR)/$@.o -o "$(OUTDIR)/$@" $(LIBDIR) $(LIBS)
	ln -sf ../libs/linux/libmdl_api.so $(OUTDIR)/libmdl_api.so

.PHONY: clean
clean:
	-cd $(OUTDIR) && \
	rm -f $(OBJS) $(DEMOS) \
	rm -f libmdl_api.so


