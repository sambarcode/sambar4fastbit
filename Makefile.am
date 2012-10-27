SUBDIRS = src java 
ACLOCAL_AMFLAGS=-I m4
jar: all 
	@echo Building jar
	@jar -cf com.globalpark.fastbit.jar com


install-data-hook: jar
	@mkdir -p $(pkgdatadir)
	@cp com.globalpark.fastbit.jar $(pkgdatadir)/
#clean-local:
#	rm -rf com
#	rm -rf com.globalpark.fastbit.jar
