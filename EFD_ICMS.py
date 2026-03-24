"""
EFD Extrator C100 + C170 — Entradas & Saídas
Ferramenta para extrair registros C100 e C170 de arquivos EFD ICMS/IPI e EFD Contribuições,
gerando planilha XLSX organizada com abas de Entradas e Saídas.
Inclui NCM e CEST do registro 0200 vinculado ao item.
"""
# ─── AUTO-INSTALL DE DEPENDÊNCIAS ────────────────────────────────────────────
import subprocess, sys

def _install(pkg):
    try:
        __import__(pkg)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "--quiet"])

_install("openpyxl")

# ─── IMPORTS ─────────────────────────────────────────────────────────────────
import streamlit as st
import tempfile, zipfile, os, io, time
from pathlib import Path

# ─── LAYOUTS OFICIAIS SPED ───────────────────────────────────────────────────
# Registro 0200 — Tabela de Identificação do Item
# |0200|COD_ITEM|DESCR_ITEM|COD_BARRA|COD_ANT_ITEM|UNID_INV|TIPO_ITEM|COD_NCM|EX_IPI|COD_GEN|COD_LST|ALIQ_ICMS|CEST|
IDX_0200_COD_ITEM = 1
IDX_0200_COD_NCM  = 7
IDX_0200_CEST     = 12

# Registro C100 — Documento Fiscal (Cód. 01, 1B, 04, 55, 65)
C100_FIELDS = [
    "REG", "IND_OPER", "IND_EMIT", "COD_PART", "COD_MOD", "COD_SIT",
    "SER", "NUM_DOC", "CHV_NFE", "DT_DOC", "DT_E_S", "VL_DOC",
    "IND_PGTO", "VL_DESC", "VL_ABAT_NT", "VL_MERC", "IND_FRT",
    "VL_FRT", "VL_SEG", "VL_OUT_DA", "VL_BC_ICMS", "VL_ICMS",
    "VL_BC_ICMS_ST", "VL_ICMS_ST", "VL_IPI", "VL_PIS", "VL_COFINS",
    "VL_PIS_ST", "VL_COFINS_ST"
]

# Registro C170 — Itens do Documento (Cód. 01, 1B, 04, 55)
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

# Campos extras vinculados do 0200
EXTRA_FIELDS = ["NCM", "CEST"]

# Índice do COD_ITEM dentro do C170 (campo 2, índice 2)
IDX_C170_COD_ITEM = 2

# Pré-calcula tamanhos para evitar chamadas repetidas
N_C100 = len(C100_FIELDS)
N_C170 = len(C170_FIELDS)
N_EXTRA = len(EXTRA_FIELDS)
N_TOTAL = N_C100 + N_C170 + N_EXTRA


# ─── PARSER OTIMIZADO (PASSAGEM ÚNICA) ───────────────────────────────────────
def parse_efd_bytes(raw: bytes) -> dict:
    """
    Parseia o EFD em uma única passagem:
      1. Constrói dicionário 0200 (COD_ITEM → NCM, CEST)
      2. Extrai C100 + C170, enriquecendo cada C170 com NCM/CEST
    Retorna dict com chaves 'entradas', 'saidas' e 'itens_0200'.
    """
    try:
        text = raw.decode("latin-1")
    except Exception:
        text = raw.decode("utf-8", errors="replace")

    lines = text.splitlines()

    # ── Fase 1: indexar todos os 0200 ────────────────────────────────────
    lookup_0200: dict[str, tuple[str, str]] = {}
    for line in lines:
        if "|0200|" not in line:
            continue
        stripped = line.strip()
        if not stripped:
            continue
        if stripped[0] == "|":
            stripped = stripped[1:]
        if stripped[-1] == "|":
            stripped = stripped[:-1]
        parts = stripped.split("|")
        if parts[0].strip().upper() != "0200":
            continue
        cod_item = parts[IDX_0200_COD_ITEM].strip() if len(parts) > IDX_0200_COD_ITEM else ""
        ncm      = parts[IDX_0200_COD_NCM].strip()  if len(parts) > IDX_0200_COD_NCM  else ""
        cest     = parts[IDX_0200_CEST].strip()      if len(parts) > IDX_0200_CEST     else ""
        if cod_item:
            lookup_0200[cod_item] = (ncm, cest)

    # ── Fase 2: extrair C100 + C170 ─────────────────────────────────────
    entradas = []
    saidas   = []
    current_c100  = None
    current_c170s = []
    current_oper  = None

    empty_extra = ("", "")

    def _flush():
        """Salva o bloco C100+C170s acumulado na lista correta."""
        nonlocal current_c100, current_c170s, current_oper
        if current_c100 is None:
            return
        rec = (current_c100, current_c170s)
        if current_oper == "0":
            entradas.append(rec)
        elif current_oper == "1":
            saidas.append(rec)
        current_c100  = None
        current_c170s = []
        current_oper  = None

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Detecta rápido se é C100 ou C170 antes de fazer split
        if "|C1" not in stripped:
            continue

        if stripped[0] == "|":
            stripped = stripped[1:]
        if stripped[-1] == "|":
            stripped = stripped[:-1]

        parts = stripped.split("|")
        reg = parts[0].strip().upper()

        if reg == "C100":
            _flush()
            c100 = parts[:N_C100]
            while len(c100) < N_C100:
                c100.append("")
            current_c100 = c100
            current_oper = parts[1].strip() if len(parts) > 1 else ""

        elif reg == "C170" and current_c100 is not None:
            c170 = parts[:N_C170]
            while len(c170) < N_C170:
                c170.append("")
            # Enriquece com NCM e CEST do 0200
            cod_item = c170[IDX_C170_COD_ITEM].strip()
            extra = lookup_0200.get(cod_item, empty_extra)
            c170.append(extra[0])  # NCM
            c170.append(extra[1])  # CEST
            current_c170s.append(c170)

    _flush()

    return {
        "entradas":  entradas,
        "saidas":    saidas,
        "itens_0200": len(lookup_0200),
    }


# ─── EXTRAÇÃO DE ARQUIVO ────────────────────────────────────────────────────
def extract_file_from_upload(uploaded) -> bytes | None:
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
                st.info(f"Encontrados {len(txt_files)} .txt no ZIP. Usando: {txt_files[0]}")
            return zf.read(txt_files[0])

    elif name.endswith(".rar"):
        try:
            _install("rarfile")
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
                    st.info(f"Encontrados {len(txt_files)} .txt no RAR. Usando: {txt_files[0]}")
                data = rf.read(txt_files[0])
            os.unlink(tmp_path)
            return data
        except Exception as e:
            st.error(f"Erro ao extrair RAR: {e}")
            return None
    else:
        return raw


# ─── GERADOR XLSX ────────────────────────────────────────────────────────────
def build_xlsx(data: dict) -> bytes:
    """Gera XLSX com abas Entradas e Saídas, incluindo NCM e CEST."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.cell import WriteOnlyCell
    from openpyxl.utils import get_column_letter

    c100_headers  = [f"C100_{c}" for c in C100_FIELDS]
    c170_headers  = [f"C170_{c}" for c in C170_FIELDS]
    extra_headers = ["0200_NCM", "0200_CEST"]
    all_headers   = c100_headers + c170_headers + extra_headers

    empty_c170 = [""] * (N_C170 + N_EXTRA)

    # Estilos pré-criados
    font_h    = Font(name="Arial", bold=True, color="FFFFFF", size=10)
    fill_c100 = PatternFill("solid", fgColor="1F4E79")
    fill_c170 = PatternFill("solid", fgColor="2E75B6")
    fill_ext  = PatternFill("solid", fgColor="6B21A8")
    align_c   = Alignment(horizontal="center", vertical="center")
    font_d    = Font(name="Arial", size=9)
    fill_z    = PatternFill("solid", fgColor="F2F7FB")

    wb = Workbook(write_only=True)

    def write_sheet(title: str, records: list):
        ws = wb.create_sheet(title=title)

        # ── Cabeçalho ────────────────────────────────────────────────────
        header_cells = []
        for i, h in enumerate(all_headers):
            cell = WriteOnlyCell(ws, value=h)
            cell.font = font_h
            cell.alignment = align_c
            if i < N_C100:
                cell.fill = fill_c100
            elif i < N_C100 + N_C170:
                cell.fill = fill_c170
            else:
                cell.fill = fill_ext
            header_cells.append(cell)
        ws.append(header_cells)

        # ── Dados ────────────────────────────────────────────────────────
        row_num = 0
        use_zebra = False
        for c100, c170s in records:
            if not c170s:
                row_num += 1
                use_zebra = not use_zebra
                row_vals = c100 + empty_c170
                cells = []
                for val in row_vals[:N_TOTAL]:
                    cell = WriteOnlyCell(ws, value=val)
                    cell.font = font_d
                    if use_zebra:
                        cell.fill = fill_z
                    cells.append(cell)
                ws.append(cells)
            else:
                for item in c170s:
                    row_num += 1
                    use_zebra = not use_zebra
                    row_vals = c100 + item
                    cells = []
                    for val in row_vals[:N_TOTAL]:
                        cell = WriteOnlyCell(ws, value=val)
                        cell.font = font_d
                        if use_zebra:
                            cell.fill = fill_z
                        cells.append(cell)
                    ws.append(cells)

        if row_num == 0:
            cell = WriteOnlyCell(ws, value=f"Nenhum registro de {title} encontrado.")
            cell.font = Font(name="Arial", bold=True, size=11, color="CC0000")
            ws.append([cell])

        # ── Larguras de coluna ───────────────────────────────────────────
        for col_idx, h in enumerate(all_headers, 1):
            hu = h.upper()
            if "CHV_NFE" in hu:
                w = 48
            elif "DESCR" in hu:
                w = 32
            elif "COD_PART" in hu:
                w = 18
            elif "NCM" in hu:
                w = 14
            elif "CEST" in hu:
                w = 14
            elif "VL_" in hu or "ALIQ" in hu:
                w = 14
            elif "DT_" in hu:
                w = 12
            elif "NUM_DOC" in hu:
                w = 14
            else:
                w = max(len(h) + 2, 11)
            ws.column_dimensions[get_column_letter(col_idx)].width = min(w, 50)

    write_sheet("ENTRADAS", data["entradas"])
    write_sheet("SAÍDAS",   data["saidas"])

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ─── UTILITÁRIOS ─────────────────────────────────────────────────────────────
def detect_efd_type(raw: bytes) -> str:
    """Detecta se é EFD ICMS/IPI ou EFD Contribuições pelo registro 0000."""
    try:
        text = raw.decode("latin-1")
    except Exception:
        text = raw.decode("utf-8", errors="replace")
    for line in text.splitlines():
        if "|0000|" not in line:
            continue
        parts = line.split("|")
        for p in parts:
            p = p.strip()
            if p in ("007","008","009","010","011","012","013",
                     "014","015","016","017","018","019","020"):
                return "EFD ICMS/IPI"
        return "EFD Contribuições"
    return "Não identificado"


def count_records(raw: bytes) -> dict:
    """Conta linhas C100, C170 e 0200 rapidamente."""
    try:
        text = raw.decode("latin-1")
    except Exception:
        text = raw.decode("utf-8", errors="replace")
    c100 = c170 = r0200 = 0
    for line in text.splitlines():
        stripped = line.strip()
        if   stripped.startswith("|C100|"): c100  += 1
        elif stripped.startswith("|C170|"): c170  += 1
        elif stripped.startswith("|0200|"): r0200 += 1
    return {"C100": c100, "C170": c170, "0200": r0200}


# ─── UI STREAMLIT ────────────────────────────────────────────────────────────
def main():
    st.set_page_config(
        page_title="EFD Extrator C100+C170",
        page_icon="📊",
        layout="wide"
    )

    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    .main .block-container { max-width: 900px; padding-top: 2rem; }
    .hero-title {
        font-family: 'Inter', sans-serif; font-size: 2rem; font-weight: 700;
        color: #1F4E79; text-align: center; margin-bottom: 0.2rem;
    }
    .hero-sub {
        font-family: 'Inter', sans-serif; font-size: 1rem;
        color: #666; text-align: center; margin-bottom: 2rem;
    }
    .stat-card {
        background: linear-gradient(135deg, #f8fafc, #eef2f7);
        border: 1px solid #e2e8f0; border-radius: 12px;
        padding: 1.2rem; text-align: center;
    }
    .stat-card h3 {
        font-family: 'Inter', sans-serif; font-size: 1.8rem;
        font-weight: 700; color: #1F4E79; margin: 0;
    }
    .stat-card p {
        font-family: 'Inter', sans-serif; font-size: 0.85rem;
        color: #64748b; margin: 0.3rem 0 0 0;
    }
    .info-badge {
        display: inline-block; background: #e0f2fe; color: #0369a1;
        font-family: 'Inter', sans-serif; font-size: 0.8rem; font-weight: 600;
        padding: 0.3rem 0.8rem; border-radius: 20px; margin: 0.2rem;
    }
    div[data-testid="stFileUploader"] {
        border: 2px dashed #2E75B6 !important;
        border-radius: 12px !important; padding: 1rem !important;
    }
    .stDownloadButton > button {
        background: linear-gradient(135deg, #1F4E79, #2E75B6) !important;
        color: white !important; font-weight: 600 !important;
        border-radius: 10px !important; padding: 0.7rem 2rem !important;
        border: none !important; width: 100% !important; font-size: 1rem !important;
    }
    .stDownloadButton > button:hover {
        background: linear-gradient(135deg, #163a5c, #1F4E79) !important;
    }
    footer {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="hero-title">📊 EFD Extrator C100 + C170</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-sub">Extraia registros de Notas Fiscais do seu SPED em segundos</div>', unsafe_allow_html=True)

    st.markdown("""
    <div style="text-align:center; margin-bottom: 1.5rem;">
        <span class="info-badge">EFD ICMS/IPI</span>
        <span class="info-badge">EFD Contribuições</span>
        <span class="info-badge">TXT • ZIP • RAR</span>
        <span class="info-badge">XLSX Formatado</span>
        <span class="info-badge">NCM + CEST (0200)</span>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    uploaded = st.file_uploader(
        "📂 Importe o arquivo EFD",
        type=["txt", "zip", "rar"],
        help="Aceita arquivos .txt, .zip ou .rar contendo o arquivo EFD"
    )

    if uploaded is not None:
        with st.spinner("Extraindo arquivo..."):
            raw = extract_file_from_upload(uploaded)

        if raw is None:
            st.stop()

        efd_type = detect_efd_type(raw)
        counts   = count_records(raw)
        file_size_mb = len(raw) / (1024 * 1024)

        tipo_label = "ICMS/IPI" if "/" in efd_type else efd_type.split()[-1]

        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            st.markdown(f'<div class="stat-card"><h3>{tipo_label}</h3><p>Tipo EFD</p></div>', unsafe_allow_html=True)
        with c2:
            st.markdown(f'<div class="stat-card"><h3>{counts["C100"]:,}</h3><p>Registros C100</p></div>', unsafe_allow_html=True)
        with c3:
            st.markdown(f'<div class="stat-card"><h3>{counts["C170"]:,}</h3><p>Registros C170</p></div>', unsafe_allow_html=True)
        with c4:
            st.markdown(f'<div class="stat-card"><h3>{counts["0200"]:,}</h3><p>Itens (0200)</p></div>', unsafe_allow_html=True)
        with c5:
            st.markdown(f'<div class="stat-card"><h3>{file_size_mb:.1f} MB</h3><p>Tamanho</p></div>', unsafe_allow_html=True)

        if counts["C100"] == 0:
            st.warning("⚠️ Nenhum registro C100 encontrado. Verifique se é um arquivo EFD válido.")
            st.stop()

        st.divider()

        if st.button("⚡ Processar e Gerar Planilha", use_container_width=True, type="primary"):
            t0 = time.time()

            with st.spinner("Processando registros..."):
                data = parse_efd_bytes(raw)

            n_ent = len(data["entradas"])
            n_sai = len(data["saidas"])
            n_itens_ent = sum(len(c170s) for _, c170s in data["entradas"])
            n_itens_sai = sum(len(c170s) for _, c170s in data["saidas"])

            with st.spinner("Gerando planilha XLSX..."):
                xlsx_bytes = build_xlsx(data)

            elapsed = time.time() - t0

            st.success(f"✅ Processado em {elapsed:.1f}s — {data['itens_0200']:,} itens no cadastro 0200 vinculados")

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

            base_name   = Path(uploaded.name).stem
            output_name = f"{base_name}_C100_C170.xlsx"

            st.download_button(
                label=f"📥 Baixar {output_name}",
                data=xlsx_bytes,
                file_name=output_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

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
