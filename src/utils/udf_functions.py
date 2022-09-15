#from pyspark.sql import DataFrame
from pyspark.sql.functions import udf

@udf
def get_HourlySalaryUDF(SalaryRangeFrom, SalaryRangeTo, SalaryFrequency) -> float:
    """
        Description:
    
        Calculate SalaryMixedFreq as follows:
            When "Salary Range From" is not null and not 0, then calculate the midrange salary, i.e. sal = (min + (max - min)/2)
            else use "Salary Range To" as the salary
            If SalaryRangeTo is not defined or 0 return None

        Calculate HourlySalary as follows:
        
           SalaryMixedFreq will be the yearly salary, Daily salary or 
           hourly salary depending on "Salary Frequency"
           Calculate HourlySalary based on "Salary Frequency":
           1 - "Hourly": set HourlySalary = SalaryMixedFreq
           2 - "Daily":  set HourlySalary = SalaryMixedFreq / 8 rounded to 2 dec places
           3 - "Annual": set HourlySalary = SalaryMixedFreq / 1757 rounded to 2 dec places

        :param SalaryRangeFrom: col value of "Salary Range From"
        :param SalaryRangeTo" col value of "Salary Range To"
        :param SalaryFrequency" col value of "Salary Frequency"
        
        :return: HourlySalary as float
    """
    SalaryMixedFreq = float(0)

    if SalaryRangeTo is None:
        return None    
    if SalaryRangeTo == 0:
        return None
    if SalaryRangeFrom is None:      
        SalaryMixedFreq = float(SalaryRangeTo)
    elif SalaryRangeFrom == 0:
        SalaryMixedFreq = float(SalaryRangeTo)
    else:
        SalaryMixedFreq = float(SalaryRangeFrom + (SalaryRangeTo - SalaryRangeFrom)/2)
    # 
    # Now we have SalaryMixedFreq, scale it dependent on Salary Frequency
    #

    if SalaryFrequency == "Hourly":
        return SalaryMixedFreq
    if SalaryFrequency == "Daily":
        return round(SalaryMixedFreq / 8,2)
    if SalaryFrequency == "Annual":
        return round(SalaryMixedFreq / 1757,2)
    # we should not get here as there are only 3 distinct SalaryFrequency's
    # but for completeness: 
    return None




