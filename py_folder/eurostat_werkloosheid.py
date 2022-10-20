import eurostat 
import pandas as pd

# create a dictionary with land codes and country names of EU27 countries in Dutch
landcodes_EU = {'AT': 'Oostenrijk', 'BE': 'België', 'BG': 'Bulgarije', 'CY': 'Cyprus', 'CZ': 'Tsjechië', 'DE': 'Duitsland', 'DK': 'Denemarken', 'EE': 'Estland', 'ES': 'Spanje', 'FI': 'Finland', 'FR': 'Frankrijk', 'EL': 'Griekenland', 'HR': 'Kroatië', 'HU': 'Hongarije', 'IE': 'Ierland', 'IT': 'Italië', 'LT': 'Litouwen', 'LU': 'Luxemburg', 'LV': 'Letland', 'MT': 'Malta', 'NL': 'Nederland', 'PL': 'Polen', 'PT': 'Portugal', 'RO': 'Roemenië', 'SE': 'Zweden', 'SI': 'Slovenië', 'SK': 'Slowakije', 'EU27_2020': 'Europese Unie'}
# get keys from landcodes_EU dictionary
landcodes = list(landcodes_EU.keys())

code = 'UNE_RT_M'
df = eurostat.get_data_df(code)