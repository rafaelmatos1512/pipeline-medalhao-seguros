"""
Popula o schema `origem` com dados fictícios.

Faz o papel do sistema legado: gera corretoras, propostas e uma fotografia
de pendências, de propósito com os defeitos que a camada silver precisa
limpar (CPF com pontuação, datas em dois formatos, status em caixa mista,
campos vazios como '' e não NULL).

Nada aqui vem de dados reais. Nomes, CNPJs, documentos, e-mails e telefones
são sorteados; os números de documento não são válidos de propósito.

Uso:
    python ingestao/gerar_dados_sinteticos.py                 # carga inicial
    python ingestao/gerar_dados_sinteticos.py --atualizar     # muda algumas linhas
    python ingestao/gerar_dados_sinteticos.py --corretoras 10 --propostas 300 --seed 7

O --atualizar existe para você ver o sync incremental do Airbyte funcionando:
ele altera o status de algumas propostas e o trigger de `atualizado_em` faz o
cursor andar.
"""

import argparse
import os
import random
import sys
from datetime import date, datetime, timedelta

import psycopg
from dotenv import load_dotenv

NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fabio", "Giovana", "Hugo", "Iara", "Joao",
         "Karen", "Lucas", "Marina", "Nelson", "Olivia", "Paulo", "Rita", "Sergio", "Tania", "Victor"]
SOBRENOMES = ["Almeida", "Barros", "Cardoso", "Dias", "Esteves", "Ferraz", "Gomes", "Horta",
              "Lima", "Machado", "Nogueira", "Pires", "Ramos", "Silva", "Tavares", "Vidal"]
PRODUTOS = [("Plano Vida Basico", "Vida"), ("Plano Vida Plus", "Vida"),
            ("Plano Previdencia A", "Previdência"), ("Plano Previdencia B", "Previdência")]
FORMAS = ["CREDIT", "DEBITO", "BOLETO", "OUTRO"]
STATUS_PROPOSTA = ["Implantada", "Pendente", "Aguardando pagamento", "Recusada", "Cancelada"]
STATUS_PAGAMENTO = ["QUITADA", "PENDENTE", "ATIVA", "CANCELADA", "DEVOLVIDA", "-"]
MOTIVOS = ["", "", "", "Documento ilegível", "Assinatura divergente", "Aguardando comprovante"]


def digitos(n: int) -> str:
    return "".join(random.choice("0123456789") for _ in range(n))


def cnpj_ficticio() -> tuple[str, str]:
    d = digitos(14)
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}", d


def cpf_ficticio() -> str:
    d = digitos(11)
    return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"


def nome_completo() -> str:
    return f"{random.choice(NOMES)} {random.choice(SOBRENOMES)} {random.choice(SOBRENOMES)}".upper()


def gerar_corretoras(cur, quantidade: int) -> list[tuple[str, str]]:
    corretoras = []
    for i in range(1, quantidade + 1):
        cnpj, limpo = cnpj_ficticio()
        razao = f"Corretora Exemplo {i:02d} Ltda"
        cur.execute(
            """INSERT INTO origem.corretoras (cnpj, cnpj_limpo, razao_social, tipo)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (cnpj_limpo) DO UPDATE SET razao_social = EXCLUDED.razao_social
               RETURNING cnpj, razao_social""",
            (cnpj, limpo, razao, "matriz" if i == 1 else random.choice(["matriz", "filial"])),
        )
        corretoras.append(cur.fetchone())
    return corretoras


def gerar_propostas(cur, corretoras, por_corretora: int) -> None:
    hoje = date.today()
    for cnpj, razao in corretoras:
        for _ in range(por_corretora):
            produto, linha = random.choice(PRODUTOS)
            criada = hoje - timedelta(days=random.randint(5, 240))
            referencia = criada + timedelta(days=random.randint(0, 30))
            vencimento = referencia + timedelta(days=random.randint(-20, 60))
            cur.execute(
                """INSERT INTO origem.propostas (
                       corretora_cnpj, corretora_razao_social, numero_proposta, nome_cliente,
                       documento_cliente, produto, linha_negocio, data_criacao, data_referencia,
                       data_vencimento, status_proposta, forma_pagamento, status_pagamento,
                       motivo_pendencia, valor)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (corretora_cnpj, numero_proposta) DO NOTHING""",
                (cnpj, razao, digitos(12), nome_completo(), cpf_ficticio(), produto.upper(), linha,
                 criada.strftime("%d/%m/%Y"), referencia.strftime("%d/%m/%Y"),
                 vencimento.strftime("%Y-%m-%d"),
                 random.choice(STATUS_PROPOSTA), random.choice(FORMAS),
                 random.choice(STATUS_PAGAMENTO), random.choice(MOTIVOS),
                 round(random.uniform(40, 900), 2)),
            )


def gerar_pendencias(cur, corretoras, por_corretora: int) -> str:
    snapshot = datetime.now().strftime("%Y%m%d%H%M%S")
    hoje = date.today()
    for cnpj, razao in corretoras:
        for _ in range(por_corretora):
            produto, linha = random.choice(PRODUTOS)
            atraso = random.randint(1, 120)
            vencimento = hoje - timedelta(days=atraso)
            nome = nome_completo()
            cur.execute(
                """INSERT INTO origem.pendencias (
                       snapshot_id, corretora_cnpj, corretora_razao_social, numero_certificado,
                       numero_proposta, documento_cliente, nome_cliente, linha_negocio, produto,
                       competencia, data_vencimento, valor_parcela, dias_atraso, forma_pagamento,
                       status_pagamento, email, telefone)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING""",
                (snapshot, cnpj, razao, digitos(10), digitos(12), cpf_ficticio(), nome, linha,
                 produto.upper(), vencimento.strftime("%m/%Y"), vencimento.strftime("%d/%m/%Y"),
                 round(random.uniform(40, 900), 2), atraso, random.choice(FORMAS), "PENDENTE",
                 f"cliente{digitos(5)}@exemplo.test", f"21 9{digitos(4)}-{digitos(4)}"),
            )
    return snapshot


def atualizar_algumas(cur) -> int:
    cur.execute(
        """UPDATE origem.propostas
              SET status_pagamento = 'QUITADA', status_proposta = 'Implantada'
            WHERE id IN (SELECT id FROM origem.propostas
                          WHERE status_pagamento = 'PENDENTE' ORDER BY random() LIMIT 10)"""
    )
    return cur.rowcount


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corretoras", type=int, default=5)
    ap.add_argument("--propostas", type=int, default=120, help="por corretora")
    ap.add_argument("--pendencias", type=int, default=25, help="por corretora")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--atualizar", action="store_true", help="altera propostas existentes em vez de criar")
    args = ap.parse_args()

    load_dotenv()
    url = os.getenv("ORIGEM_URL")
    if not url:
        print("Defina ORIGEM_URL no .env (veja o .env.example).", file=sys.stderr)
        return 1

    random.seed(args.seed)
    with psycopg.connect(url) as conn, conn.cursor() as cur:
        if args.atualizar:
            print(f"{atualizar_algumas(cur)} propostas atualizadas.")
        else:
            corretoras = gerar_corretoras(cur, args.corretoras)
            gerar_propostas(cur, corretoras, args.propostas)
            snapshot = gerar_pendencias(cur, corretoras, args.pendencias)
            print(f"{len(corretoras)} corretoras, até {args.propostas} propostas cada, snapshot {snapshot}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
