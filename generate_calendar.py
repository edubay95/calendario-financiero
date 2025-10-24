"""
generate_calendar.py
Versión mejorada:
 - Incluye Dividendos, Ex-Dividendos y Resultados (pasados y futuros)
 - Colores distintos (3 calendarios separados)
 - Corrige tickers británicos (peniques → libras)
 - Retención EE.UU. al 30%
 - SOLUCIONADO: Añade UID estable para evitar duplicados
 - SOLUCIONADO: Obtiene historial completo de eventos
 - SOLUCIONADO (Futuros): Prioriza 'tk.calendar' para futuros confirmados
 - SOLUCIONADO (Error): Corrige el error 'dict' object has no attribute 'empty'
 - SOLUCIONADO (Error): Corrige el error por 'float' (nan) en tk.calendar 
   para tickers como ASTS o FTNT.
 - NUEVO: Filtra eventos para mostrar solo ± 3 meses
"""

import os
import yfinance as yf
import pandas as pd
import requests
from ics import Calendar, Event
from datetime import datetime, date, timedelta
from dateutil import parser
from dateutil.relativedelta import relativedelta
import math

# ---------- CONFIG ----------

HOLDINGS_CSV = "holdings.csv"
EUR = "EUR"

# Retenciones por país
FOREIGN_WITHHOLDING = {
    "ES": 0.19,
    "US": 0.30,  # Ajuste: 30% retención USA
    "GB": 0.00,
    "FR": 0.128,
    "BR": 0.15,
    "SE": 0.30
}
DEFAULT_FOREIGN_WITHHOLDING = 0.15
SPANISH_RATE = 0.19

# Monedas por mercado
MARKET_CURRENCY = {
    "BME": "EUR",
    "EPA": "EUR",
    "LON": "GBP",
    "STO": "SEK",
    "NASDAQ": "USD",
    "NYSE": "USD"
}

# ---------- FUNCIONES ----------

def get_fx_rate(base_currency: str, target_currency: str = EUR):
    """Consulta tipo de cambio usando open.er-api.com"""
    base_currency = (base_currency or "EUR").upper()
    target_currency = target_currency.upper()
    if base_currency == target_currency:
        return 1.0

    try:
        url = f"https://open.er-api.com/v6/latest/{base_currency}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("result") == "success" and target_currency in data.get("rates", {}):
            return float(data["rates"][target_currency])
        else:
            print(f"[AVISO] No se encontró tipo de cambio {base_currency}->{target_currency}, se usa 1.0")
            return 1.0
    except Exception as e:
        print(f"[AVISO] Error tipo de cambio {base_currency}->{target_currency}: {e}. Se usa 1.0.")
        return 1.0


def compute_net(gross_total, rate_foreign, spanish_rate=SPANISH_RATE):
    """Calcula retención extranjera + española"""
    withholding_foreign = gross_total * rate_foreign
    spanish_theoretical = gross_total * spanish_rate
    spanish_to_pay = max(0.0, spanish_theoretical - withholding_foreign)
    net_total = gross_total - withholding_foreign - spanish_to_pay
    return {
        "gross_total": round(gross_total, 6),
        "withholding_foreign": round(withholding_foreign, 6),
        "spanish_tax_to_pay": round(spanish_to_pay, 6),
        "net_total": round(net_total, 6)
    }

def safe_parse_date(x):
    """Devuelve datetime.date o None."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    try:
        if isinstance(x, (int, float)):
            return datetime.fromtimestamp(x).date()
        if hasattr(x, 'to_pydatetime'):
            return x.to_pydatetime().date()
        if pd.isna(x):
            return None
        return parser.parse(str(x)).date()
    except Exception:
        return None


def fetch_info_for_ticker(ticker):
    """
    Obtiene info del ticker:
    - Nombre
    - Historial + Futuro (confirmado) de dividendos (pago + cantidad)
    - Historial + Futuro (confirmado) de fechas ex-dividendo
    - Historial + Futuro (confirmado) de fechas de resultados
    """
    tk = yf.Ticker(ticker)
    out = {
        'shortName': None,
        'dividends_history': [], # Lista de tuplas: (date, amount)
        'earnings_dates': [],    # Lista de dates
        'ex_dividend_dates': [] # Lista de dates
    }

    last_div_amount = 0.0

    # 1. Info general y último dividendo
    try:
        info = tk.info
        out['shortName'] = info.get('shortName') or info.get('longName')
        last_div_amount = info.get('lastDividendValue', 0.0)
    except Exception as e:
        print(f"[AVISO] {ticker}: No se pudo cargar 'info': {e}")

    # 2. --- DATOS FUTUROS (tk.calendar) ---
    try:
        cal = tk.calendar
        
        # --- ¡AQUÍ ESTÁ LA CORRECCIÓN MEJORADA! ---
        # Si 'cal' es None, lo saltamos
        if cal is None:
            raise ValueError("Calendar data is None")
        
        # Si 'cal' es un diccionario (dict) y no está vacío, lo convertimos
        if isinstance(cal, dict):
            if not cal:
                raise ValueError("Calendar data is an empty dictionary")
            cal = pd.DataFrame.from_dict(cal, orient='index')
        
        # Si 'cal' NO es un DataFrame (ej. es un float 'nan'), es inútil
        if not isinstance(cal, pd.DataFrame):
            raise TypeError(f"Calendar data is not a DataFrame or dict, but {type(cal)}")
        # ---------------------------------------------
            
        if not cal.empty:
            # Futuros Resultados
            if 'Earnings Date' in cal.index:
                ed = cal.loc['Earnings Date']
                next_earn = safe_parse_date(ed[0] if isinstance(ed, (list, pd.Series)) else ed)
                if next_earn:
                    out['earnings_dates'].append(next_earn)
            
            # Futura Fecha Ex-Dividendo
            if 'Ex-Dividend Date' in cal.index:
                exd = cal.loc['Ex-Dividend Date']
                next_ex_div = safe_parse_date(exd[0] if isinstance(exd, (list, pd.Series)) else exd)
                if next_ex_div:
                    out['ex_dividend_dates'].append(next_ex_div)

            # Futura Fecha de PAGO
            if 'Dividend Date' in cal.index:
                payd = cal.loc['Dividend Date']
                next_pay_div = safe_parse_date(payd[0] if isinstance(payd, (list, pd.Series)) else payd)
                if next_pay_div and last_div_amount > 0:
                    out['dividends_history'].append(
                        (next_pay_div, float(last_div_amount))
                    )
    except Exception as e:
        # Este print ahora mostrará el error que hemos lanzado (ej. "but <class 'float'>")
        print(f"[AVISO] {ticker}: No se pudo cargar 'calendar' (datos futuros): {e}")

    # 3. --- DATOS HISTÓRICOS ---

    # Historial de Resultados (Earnings)
    try:
        ed = tk.earnings_dates
        if ed is not None and not ed.empty:
            out['earnings_dates'].extend([safe_parse_date(d) for d in ed.index])
    except Exception as e:
        print(f"[AVISO] {ticker}: No se pudo cargar 'earnings_dates' (historial): {e}")

    # Historial de Pagos de Dividendos (Fecha de PAGO)
    try:
        divs = tk.dividends
        if not divs.empty:
            for date, amount in divs.items():
                pay_date = safe_parse_date(date)
                if pay_date:
                    out['dividends_history'].append(
                        (pay_date, float(amount))
                    )
    except Exception as e:
        print(f"[AVISO] {ticker}: No se pudo cargar 'dividends' (historial): {e}")

    # Historial de Fechas Ex-Dividendo (Fecha de CORTE)
    try:
        actions = tk.actions
        if actions is not None and 'Dividends' in actions.columns:
            ex_divs = actions[actions['Dividends'] > 0]
            for date in ex_divs.index:
                ex_date = safe_parse_date(date)
                if ex_date:
                    out['ex_dividend_dates'].append(ex_date)
    except Exception as e:
        print(f"[AVISO] {ticker}: No se pudo cargar 'actions' (ex-dividend historial): {e}")

    # Limpiar Nones y duplicados
    out['earnings_dates'] = sorted(list(set([d for d in out['earnings_dates'] if d])))
    out['ex_dividend_dates'] = sorted(list(set([d for d in out['ex_dividend_dates'] if d])))
    out['dividends_history'] = sorted(list(set(out['dividends_history'])), key=lambda x: x[0])
    
    return out


def build_events_from_holdings(csv_path=HOLDINGS_CSV):
    df = pd.read_csv(csv_path, dtype=str)
    df['cantidad'] = pd.to_numeric(df['cantidad'], errors='coerce').fillna(0).astype(int)
    events = [] 

    for _, row in df.iterrows():
        try:
            ticker = row['ticker'].strip()
            if not ticker:
                continue

            country = row.get('country', '').strip().upper()
            cantidad = int(row['cantidad'])
            name = row.get('name') or ticker
            market = row.get('market', '').strip().upper()
            if cantidad == 0:
                continue

            print(f"Procesando: {ticker} ({name})...")

            info = fetch_info_for_ticker(ticker)
            company_name = info.get('shortName') or name
            currency = MARKET_CURRENCY.get(market, "USD")

            if market == "LON":
                scale = 0.01
            else:
                scale = 1.0

            fx = get_fx_rate(currency, EUR)
            rate_foreign = FOREIGN_WITHHOLDING.get(country, DEFAULT_FOREIGN_WITHHOLDING)
            
            # 1️⃣ Ex-Dividendo (Historial + Futuro)
            for ex_date in info.get('ex_dividend_dates', []):
                desc = f"{company_name} ({ticker})\nFecha Ex-Dividendo (corte): {ex_date}"
                events.append({
                    "date": ex_date,
                    "summary": f"📅Ex-Div – {company_name}",
                    "description": desc,
                    "color": "orange",
                    "ticker": ticker 
                })

            # 2️⃣ Dividendo (Historial + Futuro)
            for div_date, div_amount in info.get('dividends_history', []):
                if div_amount and div_amount > 0:
                    gross_local = div_amount * cantidad * scale
                    gross_eur = gross_local * fx
                    calc = compute_net(gross_eur, rate_foreign, SPANISH_RATE)
                    desc = (
                        f"{company_name} ({ticker})\n"
                        f"Fecha de PAGO: {div_date}\n"
                        f"Cantidad: {cantidad} acciones\n"
                        f"Div x Acc: {div_amount * scale:.4f} {currency}\n"
                        f"--- Cálculo en EUR (FX: {fx:.4f}) ---\n"
                        f"NETO: {calc['net_total']:.2f} EUR\n"
                        f"Bruto: {calc['gross_total']:.2f} EUR\n"
                        f"Ret. Origen ({rate_foreign*100}%): -{calc['withholding_foreign']:.2f} EUR\n"
                        f"Ret. España (dif): -{calc['spanish_tax_to_pay']:.2f} EUR"
                    )
                    events.append({
                        "date": div_date,
                        "summary": f"💵Div ({calc['net_total']:.2f}€) – {company_name}",
                        "description": desc,
                        "color": "green",
                        "ticker": ticker 
                    })

            # 3️⃣ Resultados (earnings) (Historial + Futuro)
            for earn_date in info.get('earnings_dates', []):
                desc = f"{company_name} ({ticker})\nFecha de resultados: {earn_date}"
                events.append({
                    "date": earn_date,
                    "summary": f"💰Result – {company_name}",
                    "description": desc,
                    "color": "blue",
                    "ticker": ticker 
                })

        except Exception as e:
            print(f"[ERROR] {ticker}: {e}")

    return events


def write_ics_file(events_list, out_path, start_date, end_date):
    """
    Guarda una lista de eventos en un .ics, filtrando por rango de fechas.
    """
    if os.path.exists(out_path):
        os.remove(out_path)

    cal = Calendar()
    
    filtered_events = []
    for ev in events_list:
        event_date = ev.get("date")
        if event_date and (start_date <= event_date <= end_date):
            filtered_events.append(ev)

    print(f"\n[{out_path}] Eventos encontrados (total): {len(events_list)}")
    print(f"[{out_path}] Eventos guardados (en rango): {len(filtered_events)}")

    for ev in filtered_events:
        e = Event()
        e.name = ev["summary"]
        
        if not ev["date"]:
            continue
            
        e.begin = datetime.combine(ev["date"], datetime.min.time())
        e.make_all_day()
        e.description = ev["description"]

        event_type = ev['color'] 
        ticker = ev['ticker']
        date_str = ev['date'].isoformat()
        e.uid = f"{ticker}-{event_type}-{date_str}@mi.calendario.financiero.v2"
        
        e.categories = {ev.get("color", "white")} 
        cal.events.add(e)

    with open(out_path, "w", encoding="utf-8") as f:
        f.writelines(cal.serialize_iter())
    print(f"✅ Calendario actualizado: {out_path}")


# ---------- MAIN ----------

if __name__ == "__main__":
    print("Generando eventos desde holdings...")
    all_events = build_events_from_holdings(HOLDINGS_CSV)
    
    today = date.today()
    start_date = today - relativedelta(months=3)
    end_date = today + relativedelta(months=3)
    
    print(f"\nFiltrando eventos entre {start_date} y {end_date}...")
    
    div_events = [ev for ev in all_events if ev['color'] == 'green']
    ex_events = [ev for ev in all_events if ev['color'] == 'orange']
    earn_events = [ev for ev in all_events if ev['color'] == 'blue']

    write_ics_file(div_events, "dividendos_pagados.ics", start_date, end_date)
    write_ics_file(ex_events, "exdividendos.ics", start_date, end_date)
    write_ics_file(earn_events, "resultados.ics", start_date, end_date)
    

    print("\nProceso completado. Importa los 3 archivos .ics en sus respectivos calendarios de Google.")


