#!/bin/sh

Rscript opschonen.R raadsinfo _index
Rscript opschonen.R opeonoverheid_extrameta
Rscript opschonen.R openoverheid_files
Rscript opschonen.R openoverheid_meta
Rscript opschonen.R openoverheid_text_doc
Rscript opschonen.R openoverheid_text_pages