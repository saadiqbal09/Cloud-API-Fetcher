import requests
import json

# --- API Configuration ---
# Replace with your actual API details if different
API_HOST = "https://api.saicloud.in"
API_ENDPOINT = "/device_api/device_data/5"
# IMPORTANT: Replace with a valid Access-Token for your device/API
# If you have multiple tokens, you can test them one by one.
ACCESS_TOKEN = "357f30ce73e8aa0d6486e73eb645c82e1e92f0d7a367c331ef" # e.g., "930d4fb21294270b3cec468c1c7a75434aeb8f66af67c33088"

def fetch_api_parameters(token):
    """_
    Fetches data from the specified API endpoint using the provided token
    and prints all parameter_key and parameter_value pairs that are received.
    """
    url = f"{API_HOST}{API_ENDPOINT}"
    headers = {
        "Access-Token": token
    }

    print(f"\n--- Attempting to fetch data from: {url} ---")
    print(f"--- Using Access-Token (first 8 chars): {token[:8]}... ---")

    try:
        response = requests.get(url, headers=headers, timeout=15) # Increased timeout slightly

        # Check for successful HTTP response
        if response.status_code == 200:
            data = response.json()
            print("✅ Successfully received API response.")
            
            # Print the full raw JSON response for comprehensive debugging
            print("\n--- Full Raw API Response (for debugging) ---")
            print(json.dumps(data, indent=4))
            print("---------------------------------------------\n")

            # Process the 'data' array to extract parameters
            if 'data' in data and isinstance(data['data'], list) and len(data['data']) > 0:
                print("--- Extracted Parameters ---")
                for item in data['data']:
                    parameter_key = item.get('parameter_key')
                    parameter_value = item.get('parameter_value')
                    
                    if parameter_key is not None and parameter_value is not None:
                        print(f"  Key: '{parameter_key}', Value: '{parameter_value}'")
                    elif parameter_key is not None:
                        print(f"  Key: '{parameter_key}', Value: (Missing or None)")
                    else:
                        print(f"  (Skipping malformed item: {item})")
                print("----------------------------")
            else:
                print("❌ No 'data' array found in the API response or it is empty.")
                print("   This means the API did not return any specific parameter readings.")

        else:
            print(f"❌ Failed to fetch data. HTTP Status Code: {response.status_code}")
            print(f"   Response Text: {response.text}")

    except requests.exceptions.Timeout:
        print(f"🔻 Error: API request timed out after 15 seconds. URL: {url}")
    except requests.exceptions.ConnectionError:
        print(f"🔻 Error: Could not connect to the API server. Check URL or network connection: {url}")
    except requests.exceptions.RequestException as e:
        print(f"🔻 An unexpected request error occurred: {e}")
    except json.JSONDecodeError:
        print(f"🔻 Error: Could not decode JSON from API response. Response was: {response.text[:200]}...") # Print first 200 chars
    except Exception as e:
        print(f"🔻 An unexpected error occurred: {e}")

if __name__ == "__main__":
    # --- How to use ---
    # 1. Replace "YOUR_ACCESS_TOKEN_HERE" above with a real token.
    # 2. Run this script.
    # 3. Observe the "Full Raw API Response" and "Extracted Parameters" sections.

    if ACCESS_TOKEN == "":
        print("⚠️ WARNING: Please update the ACCESS_TOKEN variable in the script with your actual token.")
        print("   The script will not be able to fetch data without a valid token.")
    else:
        fetch_api_parameters(ACCESS_TOKEN)

    print("\nScript finished.")
