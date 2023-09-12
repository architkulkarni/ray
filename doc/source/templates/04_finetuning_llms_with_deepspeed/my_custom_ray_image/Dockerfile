# Use the specified image as the base
FROM rayproject/ray:nightly-py39-cu121

# Set the maintainer label
LABEL maintainer="archit@anyscale.com"

# Update apt-get and install sudo
RUN sudo apt-get update && sudo apt-get install -y git curl unzip 

# Run the pip installs
RUN pip install torch==2.0.0 torchvision==0.15.1 torchaudio==2.0.1 deepspeed==0.10.0 fairscale==0.4.13 datasets==2.14.4 accelerate==0.21.0 evaluate==0.4.0 bitsandbytes==0.41.1 wandb==0.15.8 pytorch-lightning==2.0.6 "protobuf<3.21.0" torchmetrics==1.0.3 lm_eval==0.3.0 tiktoken==0.1.2 sentencepiece==0.1.99 "urllib3<1.27" 
RUN pip install git+https://github.com/huggingface/transformers.git@d0c1aeb

# Clone the specific fork and branch, and move files
RUN git clone --branch archit-finetune-demo --depth 1 https://github.com/architkulkarni/ray.git temp-repo && \
    mkdir 04_finetuning_llms && \
    cp -r temp-repo/doc/source/templates/04_finetuning_llms_with_deepspeed/* 04_finetuning_llms/ && \
    rm -rf temp-repo

# Create the directory and change its ownership
RUN sudo mkdir /mnt/local_storage
RUN sudo chown -R ray /mnt/local_storage

# Install awscli
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN sudo ./aws/install