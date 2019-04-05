import ConfigParser

conf = ConfigParser.ConfigParser()
conf.read(["./conf/config.ini", "../conf/config.ini"])
print conf.sections()
print conf.get("task_stock_cust_return_by_prd", "travel_part_step")
print conf.items("task_stock_cust_return_by_prd")
print conf.options("task_stock_cust_return_by_prd")
