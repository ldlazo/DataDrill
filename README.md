### Run
- The pipeline can be run with command `run_pipeline.py`, 
  also, you can pass parameters as `run_pipeline.py -parameter1 val1 -parameter2 val2`, 
  such as save paths, verbose or plot and all of them are described in 
  `run_pipeline.py` in parse_opt(). You don't have to pass any of them
  if you want, because default paramters in parse_opt() are defined
- `process_bronze.py`, `process_silver.py` and `process_gold.py` are
  scripts that are called in `run_pipeline.py` and whole logic is done
  there
- each of the scripts above will save results in the corresponding folder
  `bronze`, `silver`, `gold`
- `images_reports` is place for storing plots from the gold part, it will
  save plots only if you pass paramter `-plot True`

