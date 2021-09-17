from pandas_datareader import data
import pandas as pd
import math

# Names of ETF tickers (can be extended), mentioned below are traded on eToro
tickers = ['AAXJ', 'ACWI', 'AGG', 'AMLP', 'AOA', 'AOK', 'AOR', 'BIL', 'BKLN', 'BLV', 'BND', 'BSV', 'CORN', 'CQQQ',
           'DBA', 'DBO', 'DIA', 'DJP', 'DUST', 'DVY', 'EEM', 'EFA', 'EMB', 'ERX', 'EWG', 'EWH', 'EWJ', 'EWL', 'EWN',
           'EWT', 'EWW', 'EWY', 'EWZ', 'EZU', 'FAS', 'FAZ', 'FEZ', 'FXI', 'GDX', 'GDXJ', 'GLD', 'HDV', 'HYG', 'IAEX.L',
           'IAU','ABB', 'IDEM.L', 'IEF', 'IEML.L', 'IFFF.L', 'IHI', 'IJH', 'IJJ', 'IJPE.L', 'IJR', 'ILTB', 'IMEU.L',
           'IMIB.L', 'ISF.L', 'ITOT','ITWN.L', 'IUKP.L', 'IUSA.L', 'IUSG', 'IUSV', 'IVV', 'IVW', 'IWB', 'IWF', 'IWM',
           'IWN', 'IWO', 'IWR', 'IWS', 'IXJ', 'JNK', 'KBE','KRE', 'LIT', 'LQD', 'MCHI', 'MDY', 'MINT', 'MUB', 'NUGT',
           'OIH', 'PALL', 'PFF', 'PGX', 'PHYS', 'PPLT', 'PSLV', 'QQQ', 'RSX', 'RWR', 'SCHE', 'SCHF', 'SCHX', 'SCO',
           'SDIV', 'SDOW', 'SDS', 'SEMB.L', 'SH', 'SHV', 'SHY', 'SKYY', 'SLV', 'SMH', 'SOXL', 'SOXS', 'SOXX', 'SPLV',
           'SPXL', 'SPXS', 'SPXU', 'SPY', 'SPYG', 'SQQQ', 'SRTY', 'SSO', 'SWDA.L', 'TAN', 'TFI', 'THD', 'TIP', 'TLT',
           'TMF', 'TNA', 'TQQQ', 'TVIX', 'TZA', 'UCO', 'UDOW', 'UGA', 'UNG', 'UPRO', 'USL', 'USO', 'USRT', 'VB', 'VBK',
           'VBR', 'VCIT', 'VCSH', 'VEA', 'VEU', 'VFH', 'VGK', 'VGT', 'VHT', 'VIG', 'VNQ', 'VO', 'VOE', 'VONG', 'VOO',
           'VOOG', 'VOOV', 'VOX', 'VTI', 'VTV', 'VUG', 'VWO', 'VXUS', 'XBI', 'XCX5.L', 'XLB', 'XLE', 'XLF', 'XLI',
           'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'XOP', 'XS6R.L', 'YINN']

def getData(start: str, end: str, type: str):
    """
    Function to download data for Master Thesis from Yahoo! Finance database and save it into Excel file.
    """
    # DOWNLOAD THE ADJUSTED DAILY PRICES FROM YAHOO DATABASE
    dataset = data.DataReader(tickers, 'yahoo', start, end)["Adj Close"]

    print("POST-PROCESSING THE DATA")
    # DATA CLEANING
    # if the first of the last value is nan, delete
    to_drop_name = []
    for i, column in enumerate(dataset.columns):
        try:
            # Do we have data from the beginning?
            if math.isnan(dataset[str(column)][0]):
                to_drop_name.append(column)
            # Do we have data at the end?
            if math.isnan(dataset[str(column)][-1]):
                to_drop_name.append(column)
        except:
            to_drop_name.append(column)

    dataset = dataset.drop(columns=to_drop_name, axis=1)

    # then loop and test if any data pint is missing, if yes, then manage
    for k in range(len(dataset.columns)):
        for i in range(len(dataset.index)):
            if math.isnan(float(dataset.iloc[i, k])):
                dataset.iloc[i, k] = dataset.iloc[i - 1, k].copy()

    if type == 'daily_returns':
        # we got daily prices
        dailyPrices = dataset

        # Get daily returns
        dailyReturns = dailyPrices.pct_change().drop(dailyPrices.index[0])  # drop first NaN row
        result = dailyReturns

    elif type == 'weekly_returns':
        # we got daily prices
        dailyPrices = dataset

        # GET WEEKLY RETURNS
        # Get prices only for Wednesdays and delete Nan columns
        pricesWed = dailyPrices[dailyPrices.index.weekday == 2].dropna(axis=1)

        # Get weekly returns
        weeklyReturns = pricesWed.pct_change().drop(pricesWed.index[0])  # drop first NaN row
        result = weeklyReturns

    else:
        result = dataset

    return result


if __name__ == "__main__":
    # ** Download the data from Yahoo! **
    # set up: starting date, ending date and type (price, daily_returns, weekly_returns)
    print("DOWNLOADING THE DATA, IT CAN TAKE A WHILE")
    data = getData(start='2021-05-24', end='2021-07-01', type='weekly_returns')

    print("SAVING INTO EXCEL")
    # ** save data into excel file **
    data.to_excel("yahooData.xlsx", sheet_name="data_for_thesis")

    print("THE PROCESS IS DONE")