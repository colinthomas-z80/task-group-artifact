import glob
from pandas.core.common import flatten
import parsl
from parsl import python_app, bash_app
from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import CondorProvider
from parsl.executors import HighThroughputExecutor
from parsl.executors.taskvine import TaskVineExecutor
from parsl.executors.taskvine import TaskVineFactoryConfig
from parsl.data_provider.files import File


config = Config(
        executors=[HighThroughputExecutor(),]
        )   


# used twice
@bash_app
def create_input_metadata_table(inputs=[], outputs=[], stdout='cimt.stdout', stderr='cimt.stderr'):
    return "mImgtbl . Kimages.tbl -t 1234-imglist"    

@bash_app
def create_repro_metadata_table(inputs=[], outputs=[], stdout='cpromdt.stdout', stderr='cpromdt.stderr'):
    return "mImgtbl . images.tbl -t repro_imglist"    

@bash_app
def create_fits_header(inputs=[],outputs=[], stdout='cfhdr.stdout', stderr='cfhdr.stderr'):
    return "mMakeHdr Kimages.tbl Ktemplate.hdr"    

@bash_app
def project_input_images(inputs=[],outputs=[], stdout='projii.stdout', stderr='projii.stderr'):
    return "mProjExec Kimages.tbl Ktemplate.hdr . Kstats.tbl"

@bash_app
def analyze_overlap(inputs=[],outputs=[], stdout='aolap.stdout',stderr='aolap.stderr'):
    return "mOverlaps images.tbl diffs.tbl"

@bash_app
def create_diffs(inputs=[],outputs=[], stdout='cdiff.stdout', stderr='cdiff.stderr'):
    return "mDiffExec diffs.tbl Ktemplate.hdr . ; echo dummy > diff_dummy"

@bash_app
def fit_diffs(inputs=[],outputs=[], stdout='fdiff.stdout', stderr='fdiff.stderr'):
    return "mFitExec diffs.tbl fits.tbl ."

@bash_app
def compute_corrections(inputs=[],outputs=[], stdout='compcorr.stdout', stderr='compcorr.stderr'):
    return "mBgModel images.tbl fits.tbl corrections.tbl"

@bash_app
def apply_corrections(inputs=[],outputs=[], stdout='acorr.stdout', stderr='acorr.stderr'):
    return "mBgExec images.tbl corrections.tbl . ; echo dummy > applied_dummy"

@bash_app
def add_to_mosaic(inputs=[],outputs=[], stdout='atm.stdout', stderr='atm.stderr'):
    return "mAdd images.tbl Ktemplate.hdr m17.fits"

@bash_app
def create_png(inputs=[],outputs=[], stdout='cpng.stdout', stderr='cpg.stderr'):
    return "mViewer -ct 1 -gray m17.fits -1s max gaussian-log -out m17.png"



with parsl.load(config) as conf:
    #kimages
    f_input_metadata = File("Kimages.tbl")
    f_fits_header = File("Ktemplate.hdr")
    f_imglist = File("1234-imglist")
    f_repro_imglist = File("repro_imglist")
    #kprojdir
    f_input_projection_stats = File("Kstats.tbl")
    f_overlap_diffs = File("diffs.tbl")
    f_repro_metadata = File("images.tbl")
    #diffdir
    f_fits_table = File("fits.tbl")
    f_corrections_table = File("corrections.tbl")
    f_mosaic = File("m17.fits")
    f_png = File("m17.png")
    
    # input list of fits images
    list_fits_inputs = []
    # list of file names to infer intermediate data
    list_fits_input_names = []
    for f in glob.glob('1234-aK_asky*.fits'):
        list_fits_inputs.append(File(f))
        list_fits_input_names.append(f)

    list_repro_fits_inputs = []
    for f in list_fits_input_names:
        f_strp = f.strip('./')
        list_repro_fits_inputs.append(File("hdu0_" + f_strp))
        f_area = "hdu0_" + f_strp.split(".fits")[0] + "_area.fits"
        list_repro_fits_inputs.append(File(f_area))

    list_diff_files = []
    for l in open('diff_files').readlines():
        list_diff_files.append(File(l))

    meta_table = create_input_metadata_table(inputs = [f_imglist], outputs=list(flatten([f_input_metadata, list_fits_inputs])))
    #
    fits_header = create_fits_header(inputs=[meta_table.outputs[0]], outputs=[f_fits_header])
    #
    input_projection = project_input_images(inputs=[meta_table.outputs[0], fits_header.outputs[0]], outputs=list(flatten([f_input_projection_stats, list_repro_fits_inputs])))
    #
    repro_meta_table = create_repro_metadata_table(inputs=list(flatten([input_projection.outputs, f_repro_imglist])), outputs=[f_repro_metadata])
    #    
    overlaps = analyze_overlap(inputs=[repro_meta_table.outputs[0]], outputs=[f_overlap_diffs])
    #
    diffs = create_diffs(inputs=[overlaps.outputs[0], fits_header.outputs[0]], outputs=list(flatten([File("diff_dummy"), list_diff_files])))
    #
    diff_fits = fit_diffs(inputs=list(flatten([overlaps.outputs[0], diffs.outputs])), outputs=[f_fits_table])
    #
    corrections = compute_corrections(inputs=[repro_meta_table.outputs[0], diff_fits.outputs[0]], outputs=[f_corrections_table])
    #
    applied_corrections = apply_corrections(inputs=[repro_meta_table.outputs[0], corrections.outputs[0]], outputs=[File("applied_dummy")])
    #
    mosaic_fits = add_to_mosaic(inputs=[repro_meta_table.outputs[0], fits_header.outputs[0], applied_corrections.outputs[0]], outputs=[f_mosaic])
    #
    png = create_png(inputs=[mosaic_fits.outputs[0]], outputs=[f_png])

    print(png.result())
