# $PostgreSQL$

MODULE_big = pg_controldata
DATA_built = pg_controldata.sql
DATA = uninstall_pg_controldata.sql
OBJS = pg_controldata.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_controldata
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
