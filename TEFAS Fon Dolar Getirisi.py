import pandas as pd
import plotly.express as px
import yfinance as yf
from datetime import datetime, timedelta

###############################################################################
# Step A) Generate Custom Date List
###############################################################################
def generate_date_list():
    start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")
    end_date = datetime.today()

    date_list = []
    current_date = start_date
    prev_date = None

    while current_date < end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))

        if prev_date and prev_date != start_date:
            consecutive_day = (prev_date + timedelta(days=1)).strftime("%Y-%m-%d")
            date_list.append(consecutive_day)

        prev_date = current_date
        current_date += timedelta(days=60)

    if prev_date and prev_date != start_date:
        final_consecutive_day = (prev_date + timedelta(days=1)).strftime("%Y-%m-%d")
        date_list.append(final_consecutive_day)

    date_list.append(datetime.today().strftime("%Y-%m-%d"))
    return sorted(set(date_list))

###############################################################################
# Step B) (Placeholder) Fetch TEFAS Data for Pairs of Dates
###############################################################################
def fetch_tefas_data(date_list, fund_code="YAC"):
    """
    Replace the placeholder below with your actual `tefas.fetch` logic.
    This placeholder just returns a sample DataFrame with random prices.
    """
    all_data = []
    for i in range(0, len(date_list) - 1, 2):
        # Start/end for each pair
        start = date_list[i]
        end = date_list[i + 1]

        # --- Replace with your real call to tefas.fetch ---
        data = pd.DataFrame({
            "code": [fund_code],
            "date": [start],
            "price": [100.0]  # placeholder price
        })
        # ---------------------------------------------------
        all_data.append(data)

    if not all_data:
        return pd.DataFrame(columns=["code", "date", "price"])

    final_data = pd.concat(all_data, ignore_index=True)
    final_data["date"] = pd.to_datetime(final_data["date"])
    final_data.sort_values(by="date", inplace=True, ignore_index=True)
    return final_data

###############################################################################
# Step C) Fetch USD/TRY from Yahoo Finance
###############################################################################
def fetch_usdtry_data(date_list):
    ticker = "USDTRY=X"
    start_dt = datetime.strptime(date_list[0], "%Y-%m-%d")
    end_dt = datetime.strptime(date_list[-1], "%Y-%m-%d")

    # yfinance excludes the end date, so add a day
    end_dt += timedelta(days=1)

    usdtry = yf.Ticker(ticker)
    usdtry_data = usdtry.history(
        start=start_dt.strftime("%Y-%m-%d"),
        end=end_dt.strftime("%Y-%m-%d")
    )

    usdtry_close_df = usdtry_data[["Close"]].reset_index()
    usdtry_close_df.rename(columns={"Date": "date", "Close": "USDTRY_Close"}, inplace=True)
    usdtry_close_df["date"] = pd.to_datetime(usdtry_close_df["date"]).dt.tz_localize(None)
    return usdtry_close_df

###############################################################################
# Step D) Merge TEFAS & USDTRY, Compute USD Price & Return
###############################################################################
def merge_and_compute(tefas_df, usdtry_df):
    # Merge on date
    merged = pd.merge(tefas_df, usdtry_df, on="date", how="left")

    # Compute USD_price = price / USDTRY_Close
    merged["USD_price"] = merged["price"] / merged["USDTRY_Close"]

    # Find the last valid (non-zero) USD_price
    valid_prices = merged.loc[
        merged["USD_price"].notna() & (merged["USD_price"] > 0), "USD_price"
    ]
    if valid_prices.empty:
        merged["return_to_last"] = None
        return merged

    last_valid_usd_price = valid_prices.iloc[-1]

    # Compute (last_valid_usd_price / USD_price) - 1
    merged["return_to_last"] = last_valid_usd_price / merged["USD_price"] - 1

    # Round for clarity
    merged["USD_price"] = merged["USD_price"].round(4)
    merged["return_to_last"] = merged["return_to_last"].round(4)
    return merged

###############################################################################
# Step E) Plot the Bar Chart (Only Output!)
###############################################################################
def plot_return_bar(df):
    # Extract fund code from the first row (assuming it's consistent)
    fund_code = df["code"].iloc[0] if not df.empty else "Unknown"

    fig = px.bar(
        df,
        x="date",
        y="return_to_last",
        labels={"date": "Date", "return_to_last": "Return to Last (%)"},
        title=f"{fund_code}: Dolar Bazlı Getiri (Son Günle Karşılaştırma)",
        hover_data={"return_to_last": ':.2%'}
    )
    fig.update_layout(yaxis_tickformat=".0%", xaxis_tickangle=-45)

    # The only output is the plot
    fig.show()

###############################################################################
# Main Script
###############################################################################
def main():
    # 1) Generate the custom date list
    date_list = generate_date_list()

    # 2) Fetch TEFAS data
    tefas_data = fetch_tefas_data(date_list, fund_code="YAC")  # or "ZZL", etc.

    # 3) Fetch USD/TRY from Yahoo
    usdtry_df = fetch_usdtry_data(date_list)

    # 4) Merge, compute USD-based returns
    final_df = merge_and_compute(tefas_data, usdtry_df)

    # 5) Plot the bar chart (only output)
    plot_return_bar(final_df)

if __name__ == "__main__":
    main()
