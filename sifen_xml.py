"""
Módulo de manipulação e processamento de XML para SIFEN.

Este módulo contém todas as funções relacionadas a:
- Extração de dados do XML
- Assinatura digital
- Geração de QR Code
- Conversão de certificados
- Preparação de payloads

Facilita a extensão para novos tipos de nota ao separar a lógica XML
da lógica de negócio.
"""
import base64
import hashlib
import logging
import zipfile
from datetime import datetime
from io import BytesIO
from typing import Tuple

from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    pkcs12,
)
from lxml import etree
from signxml import XMLSigner, algorithms
from signxml.signer import SignatureReference
from signxml.util import namespaces

from .config import (
    SIFEN_ENCODING,
    SIFEN_NAMESPACES,
    SIFEN_VERSION,
    URL_SIFEN_QR,
)

logger = logging.getLogger(__name__)


# ==============================================================================
# FUNÇÕES DE EXTRAÇÃO E MANIPULAÇÃO DE XML
# ==============================================================================


def extrair_cdc_do_xml(xml_assinado: str) -> str:
    """
    Extrai o CDC (Código de Controle) de 44 dígitos do XML de forma robusta.
    
    O CDC é o identificador único do documento fiscal e é necessário para
    operações como cancelamento. Esta função funciona com ou sem declaração
    de namespace, tornando-a mais robusta.
    
    Args:
        xml_assinado: XML assinado como string ou bytes
        
    Returns:
        String com o CDC (Id) de 44 dígitos, ou None se não encontrado
    """
    try:
        # Converte bytes para string se necessário
        if isinstance(xml_assinado, bytes):
            xml_assinado = xml_assinado.decode('utf-8')
        
        # Remove declaração de encoding se existir para evitar erro de parser
        xml_limpo = xml_assinado.replace(
            "<?xml version='1.0' encoding='utf-8'?>", ""
        ).strip()
        
        root = etree.fromstring(xml_limpo.encode('utf-8'))
        
        # Usa XPath para buscar a tag cujo nome local é 'DE',
        # independente do namespace. O [0] pega a primeira ocorrência
        de_nodes = root.xpath(".//*[local-name() = 'DE']")
        
        if de_nodes:
            return de_nodes[0].get("Id")
            
    except Exception as e:
        logger.error(f"Erro ao extrair CDC: {e}")
    
    return None


def converter_pfx_para_pem(pfx_path: str, senha: str) -> Tuple[str, str]:
    """
    Converte um arquivo PFX em strings PEM para a chave privada e certificado.
    
    Esta função é essencial para a assinatura digital, pois o SIFEN requer
    certificados no formato PEM, enquanto normalmente temos arquivos PFX.
    
    Args:
        pfx_path: Caminho completo para o arquivo PFX
        senha: Senha do arquivo PFX
        
    Returns:
        Tupla (private_pem, cert_pem) com as strings PEM decodificadas
        
    Raises:
        FileNotFoundError: Se o arquivo PFX não for encontrado
        ValueError: Se a senha estiver incorreta ou o arquivo for inválido
    """
    try:
        with open(pfx_path, "rb") as f:
            pfx_data = f.read()
    except FileNotFoundError:
        logger.error(f"Arquivo de certificado PFX não encontrado em: {pfx_path}")
        raise
    
    try:
        private_key, certificate, _ = pkcs12.load_key_and_certificates(
            pfx_data, senha.encode(SIFEN_ENCODING)
        )
        
        # Exporta chave privada em formato PEM
        private_pem = private_key.private_bytes(
            Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
        )
        
        # Exporta certificado público em formato PEM padrão
        cert_pem = certificate.public_bytes(Encoding.PEM)
        
        # Retorna strings decodificadas
        return private_pem.decode(SIFEN_ENCODING), cert_pem.decode(SIFEN_ENCODING)
    
    except ValueError:
        logger.error("Não foi possível carregar o PFX. A senha está correta?")
        raise


def preparar_payload_sifen(xml_preparado: str) -> str:
    """
    Prepara o payload final para envio ao SIFEN.
    
    Recebe o XML já pronto (com <rLoteDE> se necessário) e retorna
    o ZIP comprimido em Base64, que é o formato esperado pelo SIFEN.
    
    Args:
        xml_preparado: XML completo já formatado para envio
        
    Returns:
        String Base64 do arquivo ZIP contendo o XML
    """
    # Converte para bytes
    xml_bytes = xml_preparado.encode(SIFEN_ENCODING)
    
    # Comprime em ZIP em memória
    in_memory_zip = BytesIO()
    with zipfile.ZipFile(in_memory_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("documento.xml", xml_bytes)
    
    # Retorna Base64
    return base64.b64encode(in_memory_zip.getvalue()).decode(SIFEN_ENCODING)


# ==============================================================================
# FUNÇÕES DE ASSINATURA E QR CODE
# ==============================================================================


def assinar_e_gerar_qr(
    xml_original: str,
    cert_pfx_path: str,
    cert_pass: str,
    csc: str,
    csc_id: str
) -> str:
    """
    Assina o XML e gera o QR Code conforme especificação SIFEN.
    
    Esta é uma das funções mais complexas do processo, pois:
    1. Converte o certificado PFX para PEM
    2. Assina o XML digitalmente
    3. Extrai o DigestValue
    4. Gera o QR Code com hash SHA256
    5. Insere o QR Code no XML
    
    Args:
        xml_original: XML original sem assinatura
        cert_pfx_path: Caminho do certificado PFX
        cert_pass: Senha do certificado
        csc: Código de Segurança do Contribuinte (CSC)
        csc_id: ID do CSC
        
    Returns:
        XML assinado e com QR Code inserido, como string
        
    Raises:
        ValueError: Se elementos obrigatórios não forem encontrados no XML
        etree.XMLSyntaxError: Se o XML original estiver malformado
    """
    # 1. Converter PFX para PEM
    key_pem, cert_pem = converter_pfx_para_pem(cert_pfx_path, cert_pass)
    
    # 2. Parse XML
    parser = etree.XMLParser(remove_blank_text=True, ns_clean=True)
    try:
        root = etree.fromstring(
            xml_original.encode(SIFEN_ENCODING), parser=parser
        )
    except etree.XMLSyntaxError as e:
        logger.error(f"Erro ao parsear XML original antes de assinar: {e}")
        raise
    
    # 2.1. Atualiza dFecFirma com o horário atual de assinatura
    s_ns = {'s': 'http://ekuatia.set.gov.py/sifen/xsd'}
    dFecFirma_element = root.find(".//{http://ekuatia.set.gov.py/sifen/xsd}dFecFirma")
    if dFecFirma_element is not None:
        # Atualiza com o horário atual no formato ISO 8601: YYYY-MM-DDTHH:MM:SS
        data_assinatura = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        dFecFirma_element.text = data_assinatura
        logger.info(f"dFecFirma atualizado para: {data_assinatura}")
    else:
        logger.warning("Tag <dFecFirma> não encontrada no XML. Continuando sem atualizar.")
    
    # 3. Localiza <DE> e pega o Id
    de_element = root.find(".//{http://ekuatia.set.gov.py/sifen/xsd}DE")
    if de_element is None:
        raise ValueError("Tag <DE> não foi encontrada no XML.")
    de_id = de_element.get("Id")
    
    # 3.1. Validação prévia para Remissão (Tipo 7): Verifica se gTransp está presente
    iTiDE = root.findtext(".//{http://ekuatia.set.gov.py/sifen/xsd}iTiDE", namespaces=s_ns)
    if iTiDE and iTiDE.strip() == '7':
        # É uma Nota de Remissão - verifica se gTransp existe
        gTransp = root.find(".//{http://ekuatia.set.gov.py/sifen/xsd}gTransp", namespaces=s_ns)
        if gTransp is None:
            logger.warning(
                "[SIFEN] Nota de Remissão (Tipo 7) detectada, mas grupo gTransp não encontrado. "
                "O grupo gTransp é obrigatório para Notas de Remissão."
            )
    
    # 4. Assinar o XML
    signer = XMLSigner(
        signature_algorithm="rsa-sha256",
        digest_algorithm="sha256",
        c14n_algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"
    )
    
    signer.namespaces = {None: namespaces.ds}
    
    ref = SignatureReference(
        URI=f"#{de_id}",
        c14n_method=algorithms.CanonicalizationMethod.CANONICAL_XML_1_0
    )
    
    signed_root = signer.sign(
        root,
        key=key_pem,
        cert=[cert_pem],
        reference_uri=[ref]
    )
    
    # 5. Extrai o DigestValue
    ds_ns = {'sig': 'http://www.w3.org/2000/09/xmldsig#'}
    digest_value_b64 = signed_root.findtext(".//sig:DigestValue", namespaces=ds_ns)
    if not digest_value_b64:
        raise ValueError("DigestValue não encontrado no XML assinado.")
    
    # Converte DigestValue de base64 para hexadecimal
    digest_value_hex = digest_value_b64.strip().encode(SIFEN_ENCODING).hex()
    
    # 6. Extrai campos necessários para o QR
    s_ns = {'s': 'http://ekuatia.set.gov.py/sifen/xsd'}
    id_de_signed = signed_root.find(".//s:DE", namespaces=s_ns).get("Id")
    
    # Verifica se é Remissão (Tipo 7) para validar estrutura do QR
    iTiDE = signed_root.findtext(".//s:iTiDE", namespaces=s_ns)
    is_remissao = iTiDE and iTiDE.strip() == '7'
    
    def get_xml_text(path, default="0"):
        """Helper para extrair texto do XML com valor padrão."""
        val = signed_root.findtext(path, namespaces=s_ns)
        return val.strip() if val else default
    
    # Extrai todos os campos necessários para o QR
    dFeEmiDE_raw = get_xml_text(".//s:dFeEmiDE")
    dFeEmiDE_hex = dFeEmiDE_raw.encode(SIFEN_ENCODING).hex()
    dRucRec = get_xml_text(".//s:dRucRec")
    dTotGralOpe = get_xml_text(".//s:dTotGralOpe")
    dTotIVA = get_xml_text(".//s:dTotIVA")
    cItems = len(signed_root.findall(".//s:gCamItem", namespaces=s_ns))
    
    # Log informativo para Remissões
    if is_remissao:
        logger.info(
            f"[SIFEN] Gerando QR Code para Nota de Remissão (Tipo 7). "
            f"Estrutura do QR é a mesma das Faturas conforme SIFEN v1.50."
        )
    
    # 7. Monta a URL base do QR
    url_base_qr = (
        f"nVersion={SIFEN_VERSION}"
        f"&Id={id_de_signed}"
        f"&dFeEmiDE={dFeEmiDE_hex}"
        f"&dRucRec={dRucRec}"
        f"&dTotGralOpe={dTotGralOpe}"
        f"&dTotIVA={dTotIVA}"
        f"&cItems={cItems}"
        f"&DigestValue={digest_value_hex}"
        f"&IdCSC={csc_id}"
    )
    
    # 8. Gera o hash SHA256 do QR
    string_para_hash = url_base_qr + csc.strip()
    cHashQR = hashlib.sha256(
        string_para_hash.encode(SIFEN_ENCODING)
    ).hexdigest()
    
    # 9. Monta a URL final do QR
    url_final_qr = f"{URL_SIFEN_QR}{url_base_qr}&cHashQR={cHashQR}"
    
    # Logs detalhados para debug
    logger.debug(f"[SIFEN] STRING PARA HASH (url_base + CSC): {string_para_hash}")
    logger.debug(f"[SIFEN] DigestValue (base64 original): {digest_value_b64}")
    logger.debug(f"[SIFEN] DigestValue (hex usado no QR): {digest_value_hex}")
    logger.debug(f"[SIFEN] dFeEmiDE (raw): {dFeEmiDE_raw}  dFeEmiDE (hex): {dFeEmiDE_hex}")
    logger.debug(f"[SIFEN] dTotGralOpe (int): {dTotGralOpe}  dTotIVA (int): {dTotIVA}")
    logger.debug(f"[SIFEN] cHashQR: {cHashQR}")
    logger.debug(f"[SIFEN] URL Final QR: {url_final_qr}")
    
    # 10. Cria as tags do QR no XML
    dCarQR_tag = etree.Element("dCarQR")
    dCarQR_tag.text = url_final_qr
    gCamFuFD_tag = etree.Element("gCamFuFD", nsmap={None: s_ns["s"]})
    gCamFuFD_tag.append(dCarQR_tag)
    
    # 11. Insere o QR logo após </Signature>
    signature_element = signed_root.find(".//sig:Signature", namespaces=ds_ns)
    if signature_element is None:
        raise ValueError("Elemento <Signature> não encontrado no XML assinado.")
    
    parent = signature_element.getparent()
    insert_index = parent.index(signature_element)
    parent.insert(insert_index + 1, gCamFuFD_tag)
    
    # 12. Retorna XML final
    return etree.tostring(
        signed_root, encoding=SIFEN_ENCODING, xml_declaration=True
    ).decode(SIFEN_ENCODING)


# ==============================================================================
# FUNÇÕES DE EVENTOS (CANCELAMENTO)
# ==============================================================================


def gerar_evento_assinado_wsdl(
    cdc_nota: str,
    motivo: str,
    cert_path: str,
    cert_pass: str
) -> str:
    """
    Gera XML de evento de cancelamento assinado conforme especificação WSDL.
    
    Esta função cria o XML de cancelamento com:
    - ID fixo "1" (conforme correção 0141 do SIFEN)
    - Assinatura digital "irmã" (Signature como elemento irmão do rEve)
    - Namespaces travados para garantir integridade do hash
    
    Args:
        cdc_nota: CDC (Código de Controle) da nota a ser cancelada
        motivo: Motivo do cancelamento
        cert_path: Caminho do certificado PFX
        cert_pass: Senha do certificado
        
    Returns:
        XML do evento de cancelamento assinado e formatado
    """
    # ID fixo "1" conforme correção 0141 do SIFEN
    id_evento = "1"
    
    key_pem, cert_pem = converter_pfx_para_pem(cert_path, cert_pass)
    data_assinatura = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    # Namespaces
    ns_sifen = "http://ekuatia.set.gov.py/sifen/xsd"
    ns_xsi = "http://www.w3.org/2001/XMLSchema-instance"
    ns_ds = "http://www.w3.org/2000/09/xmldsig#"
    
    # Mapa completo para garantir integridade do hash
    nsmap_full = {
        None: ns_sifen,
        'xsi': ns_xsi
    }
    
    schema_loc_attr = f"{{{ns_xsi}}}schemaLocation"
    schema_loc_val = "http://ekuatia.set.gov.py/sifen/xsd siRecepEvento_v150.xsd"
    
    # 1. Monta o rEve (Objeto a ser assinado)
    rEve = etree.Element("rEve", nsmap=nsmap_full)
    rEve.set("Id", id_evento)  # Id="1"
    
    # Ordem dos campos conforme especificação
    etree.SubElement(rEve, "dFecFirma").text = data_assinatura
    etree.SubElement(rEve, "dVerFor").text = SIFEN_VERSION
    
    gGroupTiEvt = etree.SubElement(rEve, "gGroupTiEvt")
    rGeVeCan = etree.SubElement(gGroupTiEvt, "rGeVeCan")
    
    # O ID interno do cancelamento CONTINUA sendo o CDC da nota
    etree.SubElement(rGeVeCan, "Id").text = cdc_nota
    etree.SubElement(rGeVeCan, "mOtEve").text = motivo
    
    # 2. Assina o XML
    signer = XMLSigner(
        signature_algorithm="rsa-sha256",
        digest_algorithm="sha256",
        c14n_algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"
    )
    signer.namespaces = {None: ns_ds}
    
    # Assina apontando para "#1". A lib vai achar o rEve com Id="1" e assinar.
    signed_rEve = signer.sign(
        rEve, key=key_pem, cert=[cert_pem], reference_uri="#" + id_evento
    )
    
    # 3. Extrai a Signature para fora (Irmã)
    signature_element = signed_rEve.find(f".//{{{ns_ds}}}Signature")
    if signature_element is not None:
        signed_rEve.remove(signature_element)
    
    # 4. Monta a Estrutura Pai
    rGesEve = etree.Element("rGesEve", nsmap=nsmap_full)
    rGesEve.set(schema_loc_attr, schema_loc_val)
    
    rGesEve.append(signed_rEve)
    if signature_element is not None:
        rGesEve.append(signature_element)
    
    # 5. Monta o Avô
    gGroupGesEve = etree.Element("gGroupGesEve", nsmap=nsmap_full)
    gGroupGesEve.set(schema_loc_attr, schema_loc_val)
    gGroupGesEve.append(rGesEve)
    
    return etree.tostring(
        gGroupGesEve, encoding="utf-8", xml_declaration=False
    ).decode("utf-8")
