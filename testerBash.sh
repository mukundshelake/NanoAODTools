cd /home/mukund/Projects/SkimandSlim/NanoAODTools/configs
rm -rf midNov/
python renderer.py
cd /home/mukund/Projects/SkimandSlim/NanoAODTools/
python main.py -e UL2016preVFP -s selection --sample -t midNov --includeKeys Semi
