class MarketCapEligibility():
    def __init__(self, input_dataframe):
        self.input_dataframe = input_dataframe

    def filter_marketcap(self, field_str, threshold_num):

        # QC validations
        if type(field_str) != str:
            return "TypeError"
        if type(threshold_num) != int and type(threshold_num) != float:
            return "TypeError"

        df_eligible = self.input_dataframe[self.input_dataframe[field_str].astype(int) >= int(threshold_num)]
        df_ineligible = self.input_dataframe[self.input_dataframe[field_str].astype(int) < int(threshold_num)]

        return df_eligible, df_ineligible
