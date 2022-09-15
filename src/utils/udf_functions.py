from pyspark.sql import DataFrame

def get_SalaryMixedFreq(SalaryRangeFrom, SalaryRangeTo) -> float:
    """
        Description:
    
        Create SalaryMixedFreq Column as follows:
            When "Salary Range From" is not null and not 0, then calculate the midrange salary, i.e. sal = (min + (max - min)/2)
            else use "Salary Range To" as the salary

        :param SalaryRangeFrom:
        :param SalaryRangeTo:
        :return: SalaryMixedFreq

    """
    return_df = df.withColumn("SalaryMixedFreq",
                                expr("""
                                    case                              
                                        when 
                                            SalaryRangeFrom is null
                                        then SalaryRangeTo
                                        when 
                                            SalaryRangeFrom == 0
                                        then SalaryRangeTo
                                        else
                                            SalaryRangeFrom + (SalaryRangeTo - SalaryRangeFrom)/2
                                    end
                                """
                                ))
    return return_df("SalaryMixedFreq")

def get_HourlySalary(SalaryFrequency,SalaryMixedFreq) -> float:
    """
        Description:

        Create HourlySalary Column as follows:
        
           SalaryMixedFreq will be the yearly salary, Daily salary or hourly salary depending on "Salary Frequency"
           Calculate HourlySalary based on "Salary Frequency":
           1 - "Hourly": set HourlySalary = SalaryMixedFreq
           2 - "Daily":  set HourlySalary = SalaryMixedFreq / 8 rounded to 2 dec places
           3 - "Annual": set HourlySalary = SalaryMixedFreq / 1757 rounded to 2 dec places

        :param df: input dataframe

        :return: HourlySalary

    """
    return_df = df.withColumn("HourlySalary",
                                expr("""
                                    case
                                        when 
                                            SalaryFrequency == "Hourly"
                                        then SalaryMixedFreq
                                        when
                                            SalaryFrequency == "Daily"
                                        then
                                            round(SalaryMixedFreq / 8,2)
                                        else
                                            round(SalaryMixedFreq / 1757,2)
                                    end
                            """
                            ))
    return return_df("HourlySalary")

def get_degree():
    """
    regex = r'^(.*?)\s(\w*?)$'  
df \  
    .withColumn(
        'postal_code',
        regexp_extract(col('to_be_extracted'), regex, 1)
    ) \  
    .withColumn(
        'city',
        regexp_extract(col('to_be_extracted'), regex, 2)
        """
    return 1


