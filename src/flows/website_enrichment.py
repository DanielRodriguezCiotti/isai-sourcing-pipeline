from prefect import flow

from src.tasks import WebsiteEnrichmentQAInput, website_ai_parsing, website_crawling
from src.utils.logger import get_logger

BATCH_SIZE = 20


@flow(name="website_enrichment-flow")
def website_enrichment_flow(domains: list[str]):
    logger = get_logger()
    logger.info(f"Starting website enrichment for {len(domains)} domains")
    for i in range(0, len(domains), BATCH_SIZE):
        logger.info(
            f"Processing batch {i // BATCH_SIZE + 1}/{len(domains) // BATCH_SIZE}"
        )
        batch = domains[i : i + BATCH_SIZE]
        results = website_crawling(batch)
        inputs = [
            WebsiteEnrichmentQAInput(
                company_id=data.record_id, domain=domain, content=data.content
            )
            for domain, data in results.items()
        ]
        website_ai_parsing(inputs)
    logger.info("Website enrichment completed")


if __name__ == "__main__":
    domains = [
        "surface.design",
        "getleo.ai",
        "tandemai.io",
        "threedy.io",
        "ledatech.cn",
        "pcsemi.com",
        "defactotech.com",
        "colabsoftware.com",
        "holy-technologies.com",
        "zwcad.com",
        "techsoft3d.com",
        "nancal.com",
        "vorcat.com",
        "nullspaceinc.com",
        "go2cam.net",
        "ianus-simulation.de",
        "goldsim.com",
        "ecochain.com",
        "sastrarobotics.com",
        "allspice.io",
        "semisight.com",
        "riiico.com",
        "manufacturingmate.com",
        "saharacloud.io",
        "prove.design",
        "supreium.com",
        "physna.com",
        "physicsx.ai",
        "axilon.com",
        "flexcon.it",
        "3dslash.net",
        "tetmet.net",
        "durolabs.co",
        "vertex3d.com",
        "efabless.com",
        "unixde.com",
        "sophris.ai",
        "quality-line.com",
        "vayavyalabs.com",
        "partscosting.com",
        "gtisoft.com",
        "ezrobotics.com",
        "snapmagic.com",
        "thalia-da.com",
        "pnsolutions.ch",
        "eplan.com",
        "plunify.com",
        "versametrics.com",
        "visionforfood.com",
        "cenos-platform.com",
        "eleoptics.com",
        "agilines.com",
        "flux.ai",
        "laisj.com",
        "hoteamsoft.com",
        "prodea.com",
        "united-vr.com",
        "digitsvalue.com",
        "generative.vision",
        "anycasting.com",
        "aerobase.se",
        "adtechnology.com",
        "takumi-tech.com",
        "smartuq.com",
        "conceptseti.com",
        "thinkcei.com",
        "pluritec.it",
        "industrialmind.ai",
        "clous.io",
        "transvalor.com",
        "aras.com",
        "metafold3d.com",
        "intesim.com",
        "cadysolutions.com",
        "voxel-group.com",
        "emmi.ai",
        "cemworks.com",
        "zerowait-state.com",
        "getencube.com",
        "vpiphotonics.com",
        "solverx.ai",
        "hirebotics.com",
        "designfusion.com",
        "texbase.com",
        "s2ceda.com",
        "uptim.ai",
        "yangbentong.com",
        "forgis.com",
        "thirdwavesys.com",
        "preciselyso.co.uk",
        "ferritico.com",
        "diabatix.com",
        "scalenc.com",
        "open-engineering.com",
        "pcbstator.com",
        "optima-da.com",
        "edemsimulation.com",
        "nanomation.tech",
        "comsol.com",
        "orthogonal-tech.com",
    ]
    website_enrichment_flow(domains)
