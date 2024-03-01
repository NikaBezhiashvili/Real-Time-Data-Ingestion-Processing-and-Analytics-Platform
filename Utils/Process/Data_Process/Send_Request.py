
import requests


def send_row_to_api(row_data, api_endpoint):
    """POST REQUEST DEFAULT BUILD"""
    try:
        response = requests.post(api_endpoint, json=row_data)
        if response.status_code == 200:
            print("Data sent successfully.")
        else:
            print(f"Failed to send data. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
