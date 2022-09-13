from pyspark.sql import DataFrame

def get_counts_map(df: DataFrame) -> dict :
    """
        Return dict of DataFrame df's columns and their respective non-null counts. 

        Can be used to determine whether there are nulls in a dataframe, i.e.:
        If the count for each coumn != df.count() there are missing values
        (count be a neat function to do this already, and will use that when/if
        I find it, but for now this function is useful) 

        Usage: 
        df = .....        
        counts_map=get_missing_counts(df)
        print(counts_map)
        {  'Job ID': '2946',
            'Agency': '2946',
            'Posting Type': '2946',
            '# Of Positions': '2946',
            .
            .
        }

        :param df: input dataframe
        :return: dict/map of column: <count>
    """

    desc = df.describe().toPandas().transpose()
    #   summary                        count                mean              stddev   
    #   Job ID                          2946   384821.5631364562   53075.33897715407   
    #   Agency                          2946                None                None   
    #   Posting Type                    2946                None                None   
    #   # Of Positions                  2946  2.4959266802443993   9.281312826466838   
    #   Business Title                  2946                None                None   
    counts_map = { k:int(v) for k,v in desc[0].items() if k != "summary" }
    return counts_map   