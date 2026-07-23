#%%
import logging
import traceback


def save_bronze(df, output_path):
    try:
        logging.info(f"Salvando Bronze em: {output_path}")

        df.write.mode("overwrite").parquet(output_path)

        logging.info(f"Bronze salvo com sucesso em: {output_path}")

    except Exception:
        print(f"ERRO AO SALVAR BRONZE EM: {output_path}")
        traceback.print_exc()
        raise
# %%
