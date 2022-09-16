from pyspark.sql.functions import udf
#from pyspark.sql.types import FloatType

@udf(returnType='float')
def get_HourlySalaryUDF(SalaryRangeFrom, SalaryRangeTo, SalaryFrequency) -> float:
    """
        Description:

        Santity checks of accidental From To swapping:
            if SalaryRangeFrom > SalaryRangeTo
                then swap the values.

        Calculate SalaryMixedFreq as follows:
            if "Salary Range From" is not null and not 0, then calculate 
                midrange salary, i.e. sal = (min + (max - min)/2)
            else 
                use "Salary Range To" as the salary
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

    sal_from = None
    if SalaryRangeFrom is None or SalaryRangeFrom == 0:
        sal_from = 0
    else:
        sal_from = SalaryRangeFrom

    sal_to = None
    if SalaryRangeTo is None or SalaryRangeTo == 0:
        sal_to = 0
    else:
        sal_to = SalaryRangeTo

    if sal_from > sal_to:
        # swap the From. To values
        (SalaryRangeTo, SalaryRangeFrom) = (SalaryRangeFrom, SalaryRangeTo)


    SalaryMixedFreq = float(0)
    if SalaryRangeTo is None or SalaryRangeTo == 0:
        return None    
    if SalaryRangeFrom is None:      
        SalaryMixedFreq = SalaryRangeTo
    elif SalaryRangeFrom == 0:
        SalaryMixedFreq = SalaryRangeTo
    else:
        SalaryMixedFreq = SalaryRangeFrom + (SalaryRangeTo - SalaryRangeFrom)/2
    # 
    # Now we have SalaryMixedFreq, scale it dependent on Salary Frequency
    #
    HourlySalary = float(0)
    if SalaryFrequency == "Hourly":
        HourlySalary = SalaryMixedFreq
    elif SalaryFrequency == "Daily":
        HourlySalary = SalaryMixedFreq / 8
    elif SalaryFrequency == "Annual":
        HourlySalary = SalaryMixedFreq / 1757
    else:
        # we should not get here as there are only 3 distinct SalaryFrequency's
        # but for completeness: 
        HourlySalary = None

    return round(HourlySalary,2)




