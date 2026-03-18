import sys
from etl_framework.runner import main

if __name__ == "__main__":
    # runner.py expects sys.argv[1] to be config path
    # Databricks provides it in parameters already
    main()
