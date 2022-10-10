import pandas as pd

#import and save main data
cases = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.csv')
cases.to_csv('data/data_clean/health/corona/COVID19BE_CASES_AGESEX.csv')
vaccin = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_VACC.csv')
vaccin.to_csv('data/data_clean/health/corona/COVID19BE_VACC.csv')
hospital = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_HOSP.csv')
hospital.to_csv('data/data_clean/health/corona/COVID19BE_HOSP.csv')
mortality = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_MORT.csv')
mortality.to_csv('data/data_clean/health/corona/COVID19BE_MORT.csv')

# # 1. Zevendaags gemiddelde aantal bevestigde besmettingen (sinds 1 maart 2020)

cases_pivot = pd.pivot_table(cases, index = 'DATE', values = ['CASES'], aggfunc='sum')
cases_pivot['Zevendaags lopende gemiddelde'] = cases_pivot['CASES'].rolling(window=7).mean().round(decimals=0)
zevendaags = cases_pivot.drop(cases_pivot.index[range(6)])
zevendaags.reset_index(level=0, inplace=True)
zevendaags.drop(zevendaags.tail(3).index,inplace=True)
zevendaags = zevendaags.rename(columns = {'DATE': 'Datum', 'CASES': 'Bevestigde besmettingen'}, inplace = False)
zevendaags.to_csv('data/data_bewerkt/health/corona/1_besmettingen_zevendaags.csv', index=False)


# # 2. Positiviteitsratio


positiviteitsratio_url = 'https://epistat.sciensano.be/Data/COVID19BE_tests.csv'
positiviteitsratio_df = pd.read_csv(positiviteitsratio_url)
positiviteitsratio = positiviteitsratio_df.pivot_table(index = 'DATE', values = ['TESTS_ALL', 'TESTS_ALL_POS'], aggfunc = 'sum').reset_index()
positiviteitsratio['positiviteitsratio'] = positiviteitsratio['TESTS_ALL_POS'] / positiviteitsratio['TESTS_ALL']*100
positiviteitsratio['Negatieve testresultaten'] = positiviteitsratio['TESTS_ALL'] - positiviteitsratio['TESTS_ALL_POS']
positiviteitsratio = positiviteitsratio.rename(columns = {'TESTS_ALL_POS': 'Positieve testresultaten', 'DATE': 'Datum', 'TESTS_ALL': 'Aantal testen'}, inplace = False)
positiviteitsratio.to_csv('data/data_bewerkt/health/corona/2_positiviteitsratio.csv')

# # 3.  Hospitalisaties



hospital_pivot = pd.pivot_table(hospital, index = 'DATE', values = ['TOTAL_IN', 'NEW_IN'], aggfunc='sum').reset_index()
hospital_pivot = hospital_pivot.rename(columns = {'DATE': 'Datum','TOTAL_IN': 'Totaal aantal covid-patiënten in het ziekenhuis', 'NEW_IN': 'Opnames voor deze dag'}, inplace = False)
hospital_pivot.to_csv('data/data_bewerkt/health/corona/3_hospitalisaties.csv', index=True)


# # 4. ICU


icu_pivot = pd.pivot_table(hospital, index = 'DATE', values = ['TOTAL_IN_ICU'], aggfunc='sum').reset_index()
icu_pivot = icu_pivot.rename(columns = {'DATE': 'Datum', 'TOTAL_IN_ICU': 'Totaal aantal patiënten in ICU'}, inplace = False)
icu_pivot.to_csv('data/data_bewerkt/health/corona/4_total_icu.csv', index=True)

# # 5. Overlijdens


mortality_pivot = pd.pivot_table(mortality, index = 'DATE', values = 'DEATHS', aggfunc='sum').reset_index()
mortality_pivot.drop(mortality_pivot.tail(1).index,inplace=True)
mortality_pivot = mortality_pivot.rename(columns = {'DATE': 'Datum','DEATHS': 'Aantal Covid-overlijdens'}, inplace = False)
mortality_pivot['Aantal Covid-overlijdens opgeteld'] = mortality_pivot['Aantal Covid-overlijdens'].cumsum()
mortality_pivot.to_csv('data/data_bewerkt/health/corona/5_overlijdens.csv', index=True)

# # 6. Vaccinatiedekking volwassenen


population_total = 11584008.0
population_minors = 2318719.0
population_adult = population_total - population_minors
vaxdeadadultA = 10797.0
vaxdeadadultB = 44909.0
vaxdeadadultC = 3195.0
vaxdeadadultE = 12037.0
vaccinatiedekking_pivot = pd.pivot_table(vaccin, index = 'DOSE', columns = 'AGEGROUP', values = 'COUNT', aggfunc='sum').fillna(0)
vaccinatiedekking_pivot['+18'] = vaccinatiedekking_pivot['18-24'] + vaccinatiedekking_pivot['25-34'] + vaccinatiedekking_pivot['35-44'] +vaccinatiedekking_pivot['45-54'] + vaccinatiedekking_pivot['55-64'] + vaccinatiedekking_pivot['65-74'] + vaccinatiedekking_pivot['75-84'] + vaccinatiedekking_pivot['85+']
vaccinatiedekking_pivot.drop(['00-04', '05-11', '12-15', '16-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75-84', '85+'], axis=1, inplace=True)
vaccinatiedekking_pivot = vaccinatiedekking_pivot.transpose()
vaccinatiedekking_pivot['Eerste dosis abs'] = vaccinatiedekking_pivot['A'] + vaccinatiedekking_pivot['C'] - vaxdeadadultA - vaxdeadadultC
vaccinatiedekking_pivot['Volledig gevaccineerd abs'] = vaccinatiedekking_pivot['B'] + vaccinatiedekking_pivot['C'] - vaxdeadadultB - vaxdeadadultC
vaccinatiedekking_pivot['Extra dosis'] = vaccinatiedekking_pivot['E'] - vaxdeadadultE
vaccinatiedekking_pivot['Rest van de bevolking abs'] = population_adult - vaccinatiedekking_pivot['A'] - vaccinatiedekking_pivot['C'] + vaxdeadadultA +vaxdeadadultC
vaccinatiedekking_pivot['18+ met minstens eerste dosis'] = vaccinatiedekking_pivot['Eerste dosis abs'] / population_adult * 100
vaccinatiedekking_pivot['18+ volledig gevaccineerd'] = vaccinatiedekking_pivot['Volledig gevaccineerd abs'] / population_adult * 100
vaccinatiedekking_pivot['18+ met boosterprik'] = vaccinatiedekking_pivot['Extra dosis'] /population_adult * 100
vaccinatiedekking_pivot['18+ die nog geen prik kreeg'] = vaccinatiedekking_pivot['Rest van de bevolking abs'] / population_adult * 100
vaccinatiedekking_pivot['18+ met 2de boosterprik'] = vaccinatiedekking_pivot['E2'] /population_adult * 100
vaccinatiedekking_pivot['18+ met 3de boosterprik'] = vaccinatiedekking_pivot['E3'] /population_adult * 100
vaccinatiedekking_pivot['18+ die nog geen prik kreeg'] = vaccinatiedekking_pivot['Rest van de bevolking abs'] / population_adult * 100
vaccinatiedekking_pivot.drop(['A', 'B', 'C', 'E', 'E2', 'E3', 'Eerste dosis abs', 'Volledig gevaccineerd abs', 'Rest van de bevolking abs', 'Extra dosis'], axis=1, inplace=True)
vaccinatiedekking_pivot = vaccinatiedekking_pivot.transpose()
vaccinatiedekking_pivot = vaccinatiedekking_pivot.reset_index()
vaccinatiedekking_pivot = vaccinatiedekking_pivot.rename(columns = {'+18': 'Vaccinatiegraad'}, inplace = False)
vaccinatiedekking_pivot.to_csv('data/data_bewerkt/health/corona/6_vaccinatiedekking_volwassenen.csv', index=True)

# # 7. Vaccinatiedekking totale bevolking


vaxdeadallA = 10797.0
vaxdeadallB = 44919.0
vaxdeadallC = 3195.0
vaxdeadallE = 12037.0
vaccinatiedekking_total = pd.pivot_table(vaccin, columns = 'DOSE', values = 'COUNT', aggfunc='sum')
vaccinatiedekking_total['Eerste dosis'] = (vaccinatiedekking_total['A'] + vaccinatiedekking_total['C'] - vaxdeadallA - vaxdeadallC) / population_total * 100
vaccinatiedekking_total['Volledig gevaccineerd'] = (vaccinatiedekking_total['B'] + vaccinatiedekking_total['C'] - vaxdeadallB - vaxdeadallC) / population_total * 100
vaccinatiedekking_total['Boosterprik'] = (vaccinatiedekking_total['E'] - vaxdeadallE)  / population_total * 100
vaccinatiedekking_total['2de boosterprik'] = vaccinatiedekking_total['E2']  / population_total * 100
vaccinatiedekking_total['3de boosterprik'] = vaccinatiedekking_total['E3'] /population_total * 100
vaccinatiedekking_total['Ongevaccineerd'] = (population_total - vaccinatiedekking_total['A'] - vaccinatiedekking_total['C'] + vaxdeadallA + vaxdeadallC) / population_total * 100
vaccinatiedekking_total.drop(['A', 'B', 'C', 'E', 'E2', 'E3'], axis=1, inplace=True)
vaccinatiedekking_total = vaccinatiedekking_total.transpose().reset_index()
vaccinatiedekking_total = vaccinatiedekking_total.rename(columns = {'DOSE': 'Vaccinatiestatus', 'COUNT': 'Vaccinatiegraad'}, inplace = False)

vaccinatiedekking_total.to_csv('data/data_bewerkt/health/corona/7_vaccinatiedekking_totale_bevolking.csv', index=True)

# # 8. Vaccinatiedekking per regio


# Cijfers statbel 2022
duitstalig_volwassen = 63305.0
vlaams_volwassen = 5399620.0
brussels_volwassen = 948992.0
waals_volwassen = 2916677.0 - duitstalig_volwassen

#deceased after vax
deadaftervax = pd.read_csv('https://epistat.sciensano.be/Data/COVID19BE_VACC_MORT.csv')
deadaftervax.to_csv('data/data_clean/health/corona/COVID19BE_VACC_MORT.csv')
dead_pivot = pd.pivot_table(deadaftervax, index='REGION', columns='DOSE', values = 'TOT', aggfunc='sum').fillna(0).reset_index()
dead_pivot = dead_pivot.drop(3)
dead_pivot = dead_pivot.rename(columns = {'REGION': 'Regio','A': 'Adead', 'B': 'Bdead', 'C': 'Cdead', 'E': 'Edead'}, inplace = False).reset_index()
dead_pivot['Regio'] = dead_pivot['Regio'].replace(['BXL','FLA', 'GER', 'WAL'],['Brussel', 'Vlaanderen', 'Duitstalige gemeenschap', 'Wallonië'])


#vaccinated adults per region
vaccinvolwassen = vaccin.drop(vaccin[vaccin.AGEGROUP.isin(["00-04", "05-11", "12-15", "16-17", "12-15", "16-17"])].index)
vaccinvolwassen = pd.pivot_table(vaccinvolwassen, index = 'REGION', columns = 'DOSE', values = 'COUNT', aggfunc='sum').reset_index()
vaccinvolwassen['volwassen'] = [brussels_volwassen, vlaams_volwassen, duitstalig_volwassen, waals_volwassen]

vaxmindead = pd.concat([dead_pivot, vaccinvolwassen], axis=1, ignore_index=False)
vaxmindead['correctionA'] = vaxmindead['A'] - vaxmindead['Adead']
vaxmindead['correctionB'] = vaxmindead['B'] - vaxmindead['Bdead']
vaxmindead['correctionC'] = vaxmindead['C'] - vaxmindead['Cdead']
vaxmindead['correctionE'] = vaxmindead['E'] - vaxmindead['Edead']
vaxregion = vaxmindead[['Regio', 'correctionA', 'correctionB', 'correctionC', 'correctionE', 'E2', 'E3', 'volwassen']].copy()

vaxregion['Minstens één dosis'] = (vaxregion['correctionA'] + vaxregion['correctionC']) / vaxregion['volwassen'] * 100
vaxregion['Volledig gevaccineerd'] = (vaxregion['correctionB'] + vaxregion['correctionC']) / vaxregion['volwassen'] * 100
vaxregion['Boosterprik'] = vaxregion['correctionE'] / vaxregion['volwassen'] * 100
vaxregion['2de boosterprik'] = vaxregion['E2'] / vaxregion['volwassen'] * 100
vaxregion['3de boosterprik'] = vaxregion['E3'] / vaxregion['volwassen'] * 100
vaxregion = vaxregion[['Regio', 'Minstens één dosis', 'Volledig gevaccineerd', 'Boosterprik', '2de boosterprik', '3de boosterprik']]
vaxregion.to_csv('data/data_bewerkt/health/corona/8_vax_per_regio.csv', index=True)

# # 9. Vaccins cumulatief


vaccin_pivot = pd.pivot_table(vaccin, index = 'DATE', columns = 'DOSE', values = 'COUNT', aggfunc='sum').fillna(0).reset_index()
vaccin_pivot['A_cum'] = vaccin_pivot['A'].cumsum()
vaccin_pivot['B_cum'] = vaccin_pivot['B'].cumsum()
vaccin_pivot['C_cum'] = vaccin_pivot['C'].cumsum()
vaccin_pivot['E_cum'] = vaccin_pivot['E'].cumsum()
vaccin_pivot['E2_cum'] = vaccin_pivot['E2'].cumsum()
vaccin_pivot['E3_cum'] = vaccin_pivot['E3'].cumsum()
vaccin_pivot['Totaal aantal mensen volledig gevaccineerd (opgeteld)'] = vaccin_pivot['B_cum'] + vaccin_pivot['C_cum']
vaccin_pivot['Totaal aantal mensen met minstens één dosis (opgeteld)'] = vaccin_pivot['A_cum'] + vaccin_pivot['C_cum']
vaccin_pivot['Totaal aantal mensen met een boosterprik (opgeteld)'] = vaccin_pivot['E_cum']
vaccin_pivot['Totaal aantal mensen met een 2de boosterprik (opgeteld)'] = vaccin_pivot['E2_cum']
vaccin_pivot['Totaal aantal mensen met een 3de boosterprik (opgeteld)'] = vaccin_pivot['E3_cum']
vaccin_pivot.drop(['A', 'B', 'C', 'E', 'E2', 'E3', 'A_cum', 'B_cum', 'C_cum', 'E_cum', 'E2_cum', 'E3_cum'], axis=1, inplace=True)
vaccin_pivot.to_csv('data/data_bewerkt/health/corona/9_vaccin_opgeteld.csv', index=True)