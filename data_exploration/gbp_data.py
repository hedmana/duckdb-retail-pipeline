import xml.etree.ElementTree as ET
from datetime import datetime

tree = ET.parse('data/raw/gbp.xml')
root = tree.getroot()

print("=== GBP XML Structure ===")
print(f"Root: {root.tag}")

# Define namespaces
namespaces = {
    'message': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message',
    'ecb': 'http://www.ecb.europa.eu/vocabulary/stats/exr/1'
}

# Look for the actual data points
dataset = root.find('.//ecb:DataSet', namespaces)
if dataset is not None:
    # Find all Series (currency data)
    series = dataset.findall('.//ecb:Series', namespaces)
    print(f"\nFound {len(series)} currency series")

    # Look at first series (should be GBP)
    if series:
        first_series = series[0]
        print(f"First series attributes: {first_series.attrib}")

        # Find observations (actual exchange rates)
        obs = first_series.findall('.//ecb:Obs', namespaces)
        print(f"Found {len(obs)} observations")

        if obs:
            print(f"\nFirst few observations:")
            for i, observation in enumerate(obs[:5]):
                time_period = observation.get('TIME_PERIOD')
                obs_value = observation.get('OBS_VALUE')
                print(f"  {time_period}: {obs_value}")

            print(f"\nLast few observations:")
            for observation in obs[-3:]:
                time_period = observation.get('TIME_PERIOD')
                obs_value = observation.get('OBS_VALUE')
                print(f"  {time_period}: {obs_value}")
