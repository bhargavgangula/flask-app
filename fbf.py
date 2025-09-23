import pandas as pd
import urllib.parse
from tkinter import Tk, filedialog
import os
import requests

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/100.0.4896.127 Safari/537.36"
}

def build_gbp_link(name, address):
    query = f"{name} {address}"
    return "https://www.google.com/search?q=" + urllib.parse.quote(query)

def get_open_status(name, address):
    url = build_gbp_link(name, address)
    try:
        r = requests.get(url, headers=headers, timeout=10)
        text = r.text.lower()

        if "temporarily closed" in text:
            return "Temporarily closed"
        elif "permanently closed" in text:
            return "Permanently closed"
        elif "open" in text or "closes" in text or "hours" in text:
            return "Open"
        else:
            return "Unknown"
    except Exception as e:
        return f"Error: {e}"

def main():
    # File dialog to pick input file
    root = Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(
        title="Select Excel File with Company Data",
        filetypes=[("Excel Files", "*.xlsx *.xls")]
    )
    if not file_path:
        print("❌ No file selected. Exiting...")
        return

    # Load Excel
    df = pd.read_excel(file_path)

    # Validate columns
    required = ["Company Name", "Address"]
    for col in required:
        if col not in df.columns:
            print(f"❌ Missing required column: {col}")
            return

    # --- Get user input for range ---
    try:
        start_idx = int(input(f"👉 Enter start index (1 - {len(df)}): ")) - 1
        end_idx = int(input(f"👉 Enter end index (1 - {len(df)}): "))
    except ValueError:
        print("❌ Invalid index input.")
        return

    if start_idx < 0 or end_idx > len(df) or start_idx >= end_idx:
        print("❌ Invalid range.")
        return

    # Slice dataframe
    df_range = df.iloc[start_idx:end_idx].copy()

    # Build GBP links
    df_range["GBP_Link"] = df_range.apply(lambda x: build_gbp_link(str(x["Company Name"]), str(x["Address"])), axis=1)

    # Fetch Open Status
    df_range["Open_Status"] = df_range.apply(lambda x: get_open_status(str(x["Company Name"]), str(x["Address"])), axis=1)

    # Save new file
    out_path = os.path.join(os.path.dirname(file_path), f"stores_{start_idx+1}_to_{end_idx}.xlsx")
    df_range.to_excel(out_path, index=False)

    print(f"✅ Done! File saved at: {out_path}")

if __name__ == "__main__":
    main()
