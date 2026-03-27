from dotenv import load_dotenv
load_dotenv()


from chilean_real_state_offer_extraction_pipeline.src_to_brz.main import run_process as run_src_to_brz # noqa: E402

from chilean_real_state_offer_extraction_pipeline.brz_to_slv.main import run_process as run_brz_to_slv # noqa: E402

run_src_to_brz()
run_brz_to_slv()

