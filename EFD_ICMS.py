"""
EFD Extrator C100 + C170 — Entradas & Saídas
Ferramenta para extrair registros C100 e C170 de arquivos EFD ICMS/IPI e EFD Contribuições,
gerando planilha XLSX organizada com abas de Entradas e Saídas.
"""
import streamlit as st
import tempfile, zipfile, os, io, time
from pathlib import Path


# ─── LAYOUTS OFICIAIS SPED ───────────────────────────────────────────────────
# Registro C100 — Documento Fiscal (Cód. 01, 1B, 04, 55, 65)
# Conforme Guia Prático EFD ICMS/IPI v3.1.8 e EFD Contribuições v1.35
C100_FIELDS = [
    "REG", "IND_OPER", "IND_EMIT", "COD_PART", "COD_MOD", "COD_SIT",
    "SER", "NUM_DOC", "CHV_NFE", "DT_DOC", "DT_E_S", "VL_DOC",
    "IND_PGTO", "VL_DESC", "VL_ABAT_NT", "VL_MERC", "IND_FRT",
    "VL_FRT", "VL_SEG", "VL_OUT_DA", "VL_BC_ICMS", "VL_ICMS",
    "VL_BC_ICMS_ST", "VL_ICMS_ST", "VL_IPI", "VL_PIS", "VL_COFINS",
    "VL_PIS_ST", "VL_COFINS_ST"
]

# Registro C170 — Itens do Documento (Cód. 01, 1B, 04, 55)
# Layout completo conforme Guia Prático EFD ICMS/IPI v3.1.8
C170_FIELDS = [
    "REG", "NUM_ITEM", "COD_ITEM", "DESCR_COMPL", "QTD", "UNID",
    "VL_ITEM", "VL_DESC", "IND_MOV", "CST_ICMS", "CFOP", "COD_NAT",
    "VL_BC_ICMS", "ALIQ_ICMS", "VL_ICMS", "VL_BC_ICMS_ST", "ALIQ_ST",
    "VL_ICMS_ST", "IND_APUR", "CST_IPI", "COD_ENQ", "VL_BC_IPI",
    "ALIQ_IPI", "VL_IPI", "CST_PIS", "VL_BC_PIS", "ALIQ_PIS",
    "QUANT_BC_PIS", "ALIQ_PIS_QUANT", "VL_PIS", "CST_COFINS",
    "VL_BC_COFINS", "ALIQ_COFINS", "QUANT_BC_COFINS", "ALIQ_COFINS_QUANT",
    "VL_COFINS", "COD_CTA", "VL_ABAT_NT"
]

# ─── PARSER OTIMIZADO ────────────────────────────────────────────────────────
def parse_efd_bytes(raw: bytes) -> dict:
    """Parseia o conteúdo bruto do EFD em bytes, retornando entradas e saídas."""
    entradas = []  # lista de dicts {c100: [...], c170s: [[...], ...]}
    saidas = []
    current_c100 = None
    current_c170s = []
    current_oper = None

    # Decodifica tentando latin-1 (padrão SPED ASCII ISO 8859-1)
    try:
        text = raw.decode("latin-1")
    except Exception:
        text = raw.decode("utf-8", errors="replace")

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Remove pipes externos
        if line.startswith("|"):
            line = line[1:]
        if line.endswith("|"):
            line = line[:-1]

        parts = line.split("|")
        reg = parts[0].strip().upper()

        if reg == "C100":
            # Salva o C100 anterior se existir
            if current_c100 is not None:
                rec = {"c100": current_c100, "c170s": current_c170s}
                if current_oper == "0":
                    entradas.append(rec)
                elif current_oper == "1":
                    saidas.append(rec)

            # Mapeia campos do C100
            current_c100 = []
            for i, campo in enumerate(C100_FIELDS):
                current_c100.append(parts[i] if i < len(parts) else "")
            current_c170s = []
            current_oper = parts[1].strip() if len(parts) > 1 else ""

        elif reg == "C170" and current_c100 is not None:
            item = []
            for i, campo in enumerate(C170_FIELDS):
                item.append(parts[i] if i < len(parts) else "")
            current_c170s.append(item)

    # Último C100 pendente
    if current_c100 is not None:
        rec = {"c100": current_c100, "c170s": current_c170s}
        if current_oper == "0":
            entradas.append(rec)
        elif current_oper == "1":
            saidas.append(rec)

    return {"entradas": entradas, "saidas": saidas}


def extract_file_from_upload(uploaded) -> bytes:
    """Extrai o conteúdo TXT do arquivo (suporte a TXT, ZIP, RAR)."""
    name = uploaded.name.lower()
    raw = uploaded.read()

    if name.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            txt_files = [f for f in zf.namelist()
                         if f.lower().endswith(".txt") and not f.startswith("__MACOSX")]
            if not txt_files:
                st.error("Nenhum arquivo .txt encontrado dentro do ZIP.")
                return None
            if len(txt_files) > 1:
                st.info(f"Encontrados {len(txt_files)} arquivos .txt no ZIP. Usando: {txt_files[0]}")
            return zf.read(txt_files[0])

    elif name.endswith(".rar"):
        try:
            import rarfile
            with tempfile.NamedTemporaryFile(suffix=".rar", delete=False) as tmp:
                tmp.write(raw)
                tmp_path = tmp.name
            with rarfile.RarFile(tmp_path) as rf:
                txt_files = [f for f in rf.namelist() if f.lower().endswith(".txt")]
                if not txt_files:
                    st.error("Nenhum arquivo .txt encontrado dentro do RAR.")
                    return None
                if len(txt_files) > 1:
                    st.info(f"Encontrados {len(txt_files)} arquivos .txt no RAR. Usando: {txt_files[0]}")
                data = rf.read(txt_files[0])
            os.unlink(tmp_path)
            return data
        except ImportError:
            st.error("Biblioteca `rarfile` não instalada. Execute: `pip install rarfile`")
            return None
        except Exception as e:
            st.error(f"Erro ao extrair RAR: {e}")
            return None
    else:
        return raw


# ─── GERADOR XLSX ────────────────────────────────────────────────────────────


def build_xlsx(data: dict) -> bytes:
    """Gera o XLSX com abas de Entradas e Saídas usando xlsxwriter (alta performance)."""
    import xlsxwriter

    c100_headers = [f"C100_{c}" for c in C100_FIELDS]
    c170_headers = [f"C170_{c}" for c in C170_FIELDS]
    all_headers = c100_headers + c170_headers
    n_c100 = len(c100_headers)

    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {"in_memory": True, "strings_to_numbers": False})

    # Formatos
    fmt_h_c100 = wb.add_format({
        "bold": True, "font_name": "Arial", "font_size": 10,
        "font_color": "#FFFFFF", "bg_color": "#1F4E79",
        "align": "center", "valign": "vcenter",
        "border": 1, "border_color": "#D9D9D9",
        "text_wrap": True
    })
    fmt_h_c170 = wb.add_format({
        "bold": True, "font_name": "Arial", "font_size": 10,
        "font_color": "#FFFFFF", "bg_color": "#2E75B6",
        "align": "center", "valign": "vcenter",
        "border": 1, "border_color": "#D9D9D9",
        "text_wrap": True
    })
    fmt_data = wb.add_format({
        "font_name": "Arial", "font_size": 9,
        "border": 1, "border_color": "#E8E8E8"
    })
    fmt_zebra = wb.add_format({
        "font_name": "Arial", "font_size": 9,
        "bg_color": "#F2F7FB",
        "border": 1, "border_color": "#E8E8E8"
    })

    def write_sheet(ws, records, label):
        # Headers
        for col, h in enumerate(all_headers):
            fmt = fmt_h_c170 if col >= n_c100 else fmt_h_c100
            ws.write(0, col, h, fmt)

        # Freeze + filtro
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, 0, len(all_headers) - 1)

        # Dados
        row = 1
        for rec in records:
            c100 = rec["c100"]
            c170s = rec["c170s"]
            if not c170s:
                fmt = fmt_zebra if (row % 2) == 0 else fmt_data
                for col, val in enumerate(c100):
                    ws.write(row, col, val, fmt)
                row += 1
            else:
                for item in c170s:
                    fmt = fmt_zebra if (row % 2) == 0 else fmt_data
                    for col, val in enumerate(c100):
                        ws.write(row, col, val, fmt)
                    for col, val in enumerate(item):
                        ws.write(row, n_c100 + col, val, fmt)
                    row += 1

        if row == 1:
            ws.write(1, 0, f"Nenhum registro de {label} encontrado.")

        # Larguras
        for col, h in enumerate(all_headers):
            h_upper = h.upper()
            if "CHV_NFE" in h_upper:
                w = 48
            elif "DESCR" in h_upper:
                w = 32
            elif "COD_PART" in h_upper:
                w = 18
            elif "VL_" in h_upper or "ALIQ" in h_upper:
                w = 14
            elif "DT_" in h_upper:
                w = 12
            elif "NUM_DOC" in h_upper:
                w = 14
            else:
                w = max(len(h) + 2, 11)
            ws.set_column(col, col, min(w, 50))

    ws_ent = wb.add_worksheet("ENTRADAS")
    ws_sai = wb.add_worksheet("SAÍDAS")

    write_sheet(ws_ent, data["entradas"], "ENTRADAS")
    write_sheet(ws_sai, data["saidas"], "SAÍDAS")

    wb.close()
    return buf.getvalue()


# ─── UI STREAMLIT ────────────────────────────────────────────────────────────
def detect_efd_type(raw: bytes) -> str:
    """Detecta se é EFD ICMS/IPI ou EFD Contribuições pelo registro 0000."""
    try:
        text = raw.decode("latin-1")
    except Exception:
        text = raw.decode("utf-8", errors="replace")
    for line in text.splitlines():
        line = line.strip()
        if "|0000|" in line:
            parts = line.split("|")
            for p in parts:
                p = p.strip()
                if p in ("007", "008", "009", "010", "011", "012", "013",
                         "014", "015", "016", "017", "018", "019", "020"):
                    return "EFD ICMS/IPI"
            # EFD Contribuições usa códigos de leiaute diferentes
            return "EFD Contribuições"
    return "Não identificado"


def count_records(raw: bytes) -> dict:
    """Conta linhas C100 e C170 rapidamente."""
    try:
        text = raw.decode("latin-1")
    except Exception:
        text = raw.decode("utf-8", errors="replace")
    c100 = c170 = 0
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("|C100|"):
            c100 += 1
        elif stripped.startswith("|C170|"):
            c170 += 1
    return {"C100": c100, "C170": c170}


def main():
    st.set_page_config(
        page_title="EFD Extrator C100+C170",
        page_icon="📊",
        layout="wide"
    )

    # CSS customizado
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    .main .block-container {
        max-width: 900px;
        padding-top: 2rem;
    }

    .hero-title {
        font-family: 'Inter', sans-serif;
        font-size: 2rem;
        font-weight: 700;
        color: #1F4E79;
        text-align: center;
        margin-bottom: 0.2rem;
    }

    .hero-sub {
        font-family: 'Inter', sans-serif;
        font-size: 1rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }

    .stat-card {
        background: linear-gradient(135deg, #f8fafc, #eef2f7);
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 1.2rem;
        text-align: center;
    }

    .stat-card h3 {
        font-family: 'Inter', sans-serif;
        font-size: 1.8rem;
        font-weight: 700;
        color: #1F4E79;
        margin: 0;
    }

    .stat-card p {
        font-family: 'Inter', sans-serif;
        font-size: 0.85rem;
        color: #64748b;
        margin: 0.3rem 0 0 0;
    }

    .info-badge {
        display: inline-block;
        background: #e0f2fe;
        color: #0369a1;
        font-family: 'Inter', sans-serif;
        font-size: 0.8rem;
        font-weight: 600;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        margin: 0.2rem;
    }

    div[data-testid="stFileUploader"] {
        border: 2px dashed #2E75B6 !important;
        border-radius: 12px !important;
        padding: 1rem !important;
    }

    .stDownloadButton > button {
        background: linear-gradient(135deg, #1F4E79, #2E75B6) !important;
        color: white !important;
        font-weight: 600 !important;
        border-radius: 10px !important;
        padding: 0.7rem 2rem !important;
        border: none !important;
        width: 100% !important;
        font-size: 1rem !important;
    }

    .stDownloadButton > button:hover {
        background: linear-gradient(135deg, #163a5c, #1F4E79) !important;
    }

    footer {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

    # Header
    st.markdown('<div class="hero-title">📊 EFD Extrator C100 + C170</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-sub">Extraia registros de Notas Fiscais do seu SPED em segundos</div>', unsafe_allow_html=True)

    # Badges informativos
    st.markdown("""
    <div style="text-align:center; margin-bottom: 1.5rem;">
        <span class="info-badge">EFD ICMS/IPI</span>
        <span class="info-badge">EFD Contribuições</span>
        <span class="info-badge">TXT • ZIP • RAR</span>
        <span class="info-badge">XLSX Formatado</span>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # Upload
    uploaded = st.file_uploader(
        "📂 Importe o arquivo EFD",
        type=["txt", "zip", "rar"],
        help="Aceita arquivos .txt, .zip ou .rar contendo o arquivo EFD"
    )

    if uploaded is not None:
        # Extrai conteúdo
        with st.spinner("Extraindo arquivo..."):
            raw = extract_file_from_upload(uploaded)

        if raw is None:
            st.stop()

        # Informações do arquivo
        efd_type = detect_efd_type(raw)
        counts = count_records(raw)
        file_size_mb = len(raw) / (1024 * 1024)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f"""
            <div class="stat-card">
                <h3>{efd_type.split()[-1] if '/' not in efd_type else 'ICMS/IPI'}</h3>
                <p>Tipo EFD</p>
            </div>""", unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div class="stat-card">
                <h3>{counts['C100']:,}</h3>
                <p>Registros C100</p>
            </div>""", unsafe_allow_html=True)
        with col3:
            st.markdown(f"""
            <div class="stat-card">
                <h3>{counts['C170']:,}</h3>
                <p>Registros C170</p>
            </div>""", unsafe_allow_html=True)
        with col4:
            st.markdown(f"""
            <div class="stat-card">
                <h3>{file_size_mb:.1f} MB</h3>
                <p>Tamanho</p>
            </div>""", unsafe_allow_html=True)

        if counts["C100"] == 0:
            st.warning("⚠️ Nenhum registro C100 encontrado neste arquivo. Verifique se é um arquivo EFD válido.")
            st.stop()

        st.divider()

        # Processa
        if st.button("⚡ Processar e Gerar Planilha", use_container_width=True, type="primary"):
            t0 = time.time()

            with st.spinner("Processando registros..."):
                data = parse_efd_bytes(raw)

            n_ent = len(data["entradas"])
            n_sai = len(data["saidas"])
            n_itens_ent = sum(len(r["c170s"]) for r in data["entradas"])
            n_itens_sai = sum(len(r["c170s"]) for r in data["saidas"])

            with st.spinner("Gerando planilha XLSX..."):
                xlsx_bytes = build_xlsx(data)

            elapsed = time.time() - t0

            st.success(f"✅ Processado em {elapsed:.1f}s")

            # Resumo
            col_e, col_s = st.columns(2)
            with col_e:
                st.markdown(f"""
                <div class="stat-card" style="border-left: 4px solid #16a34a;">
                    <h3 style="color:#16a34a;">{n_ent}</h3>
                    <p>Notas de Entrada</p>
                    <p style="font-size:0.75rem; color:#94a3b8;">{n_itens_ent:,} itens (C170)</p>
                </div>""", unsafe_allow_html=True)
            with col_s:
                st.markdown(f"""
                <div class="stat-card" style="border-left: 4px solid #dc2626;">
                    <h3 style="color:#dc2626;">{n_sai}</h3>
                    <p>Notas de Saída</p>
                    <p style="font-size:0.75rem; color:#94a3b8;">{n_itens_sai:,} itens (C170)</p>
                </div>""", unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # Gera nome do arquivo de saída
            base_name = Path(uploaded.name).stem
            output_name = f"{base_name}_C100_C170.xlsx"

            st.download_button(
                label=f"📥 Baixar {output_name}",
                data=xlsx_bytes,
                file_name=output_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

    # Rodapé
    st.markdown("<br>", unsafe_allow_html=True)
    st.divider()
    st.markdown("""
    <div style="text-align:center; padding: 0.5rem 0;">
        <span style="font-family:'Inter',sans-serif; font-size:0.75rem; color:#94a3b8;">
            UFISCAL — Inteligência em Negócios • Layout conforme Guia Prático EFD v3.1.8 / EFD Contribuições v1.35
        </span>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
