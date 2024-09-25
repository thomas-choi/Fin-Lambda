from FOC import FOC
import pandas as pd

#create instance
ref_FOC = FOC()

def retreive_options(ticker, expiration, pc, strike):
    #fetch current stock price for AAPL
    if pc == "P":
        pc = "PUT"
    elif pc == "C":
        pc = "CALL"
    else:
        pc = ""

    try:
        contract_symbol = ref_FOC.get_contract_symbol(ticker, expiration, pc, strike)
        print("FOC.contract_symbol: ", contract_symbol)
        options_contract = ref_FOC.get_options_price_data(contract_symbol)
        data = options_contract.iloc[0].to_dict()
        print(data)
        opt={}
        opt["contractSymbol"] = data['contract_symbol']
        opt["lastTradeDate"] = ""
        opt["strike"] = strike
        opt["lastPrice"] = "0" if data['Last Sale'] == "N/A" else data['Last Sale']
        opt["bid"] = "0" if data['Bid'] == "N/A" else data['Bid']
        opt["ask"] = "0" if data['Ask'] == "N/A" else data['Ask'] 
        opt["change"] = 0
        opt["percentChange"] = 0
        opt["volume"] = "0" if data['Volume'] == "N/A" else data['Volume']
        opt["openInterest"] = "0" if data['Open Interest'] == "N/A" else data['Open Interest'] 
        opt["impliedVolatility"] = "0" if data['Impvol'] == "N/A" else data['Impvol'] 
        opt["inTheMoney"] = ""
        opt["contractSize"] = 'REGULAR'
        opt["currency"] = "USD"
        opt_rec = pd.Series(opt)
        ts = data["timestamp"]
    except Exception as error:
        print(error)
        opt_rec = None
        ts = None

    return opt_rec, ts

# get options contract symbol for AAPL CALL options with strike $200 for 6 October 2023
# contract_symbol = ref_FOC.get_contract_symbol("XOM",'2024-10-18','PUT',111)
# options_contract = ref_FOC.get_options_price_data(contract_symbol)

if __name__ == '__main__':
    ct, ts = retreive_options("XOM", '2024-10-18','P',111)
    print(ct)
    print('timestamp: ', ts)

    ct, ts = retreive_options("AMD", '2024-10-18','P',152.5)
    print(ct)
    print('timestamp: ', ts)

    ct, ts = retreive_options("KO", '2024-10-18','P', 67)
    print(ct)
    print('timestamp: ', ts)

