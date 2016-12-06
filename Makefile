.PHONY: bwa clean sparkaligner

JAR = jar
RMRF = rm -rf

MAKE = make
LOCATION = `pwd`
OUTPUT_DIR = sparkaligner_out
RESOURCES_DIR = src/main/resources

# Bwa variables ########
BWA_DIR = lib/
BWA = bwa

all: sparkaligner
	@echo "================================================================================"
	@echo "SparkAligner has been compiled."
	@echo "Location    = $(LOCATION)/$(OUTPUT_DIR)/"
	@echo "================================================================================"

bwa:
	$(MAKE) -C $(BWA_DIR)/$(BWA)
	if [ ! -d "$(RESOURCES_DIR)" ]; then mkdir $(RESOURCES_DIR); fi
	cp $(BWA_DIR)/$(BWA)/bwa $(RESOURCES_DIR)/

sparkaligner: bwa
	mvn clean package
	if [ ! -d "$(OUTPUT_DIR)" ]; then mkdir $(OUTPUT_DIR); fi
	cp target/*.jar $(OUTPUT_DIR)

clean:
	$(RMRF) target
	$(RMRF) $(OUTPUT_DIR)
	$(MAKE) clean -C $(BWA_DIR)/$(BWA)
