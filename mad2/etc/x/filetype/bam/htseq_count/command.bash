if [ ! -f  "{{ readsortout }}.bam" ];
then
  samtools sort -n -m 2G \
      "{{ fullpath }}" \
      "{{ readsortout }}" && \
  mad set status temp "{{ readsortout }}.bam";
fi;

samtools view -h "{{ readsortout }}.bam" \
  | htseq-count \
    --format=sam \
    --order=name \
    --idattr={{ htseq_idattr }} \
    --stranded={{ htseq_stranded }} \
    --mode={{ htseq_mode }} \
    --type={{ htseq_feature_type }} \
    - \
    "{{ genome_gtf_file }}" \
    > "{{ output }}"
