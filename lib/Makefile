.PHONY: bwa clean sparkaligner

JAR = jar
RMRF = rm -rf

MAKE = make
LOCATION = `pwd`
RESOURCES_DIR = ../src/main/resources

# Bwa variables ########
BWA_DIR = .
BWA = bwa

all: bwa

bwa:
	$(MAKE) -C $(BWA_DIR)/$(BWA)
	if [ ! -d "$(RESOURCES_DIR)" ]; then mkdir $(RESOURCES_DIR); fi
	cp $(BWA_DIR)/$(BWA)/bwa $(RESOURCES_DIR)/

clean:
	$(RMRF) target
	$(MAKE) clean -C $(BWA_DIR)/$(BWA)
