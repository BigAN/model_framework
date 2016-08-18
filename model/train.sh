# 复制


train_data=user_feature_raw
version=v_27_zhenghe_pingjia_jizhong
app_name=user_features
app=${app_name}_${version}
features_lines=${app_name}_${version}_features_lines
test_file=user_features_test

#过滤掉第一行
awk 'NR>2{print $0}' user_feature_raw > tmp && cp tmp user_feature_raw
awk 'NR>2{print $0}' user_features_test > tmp && cp tmp user_features_test

da && cp user_feature_raw user_feature_raw_bk
wc user_feature_raw
#da && cp user_feature_raw_bk user_feature_raw
#将neg_type 为1 3 的转为punish_status = 1, 过掉neg_type = 2
#da&& awk -F '\t' 'BEGIN{OFS="\t"}{if($(NF-1)!=2){print $0}}' user_feature_raw > tmp && mv tmp user_feature_raw# 46 NF-1
da&& awk -F '\t' 'BEGIN{OFS="\t"}{if(($46==1 ) && $46!= "NULL" && NR!=1){$NF=1;print$0}else if($46!=3){print $0}}' user_feature_raw > tmp && mv tmp user_feature_raw
#过滤数据
#da && awk -F '\t' 'BEGIN{OFS="\t"}{if($NF==1){if($31>1){print $0}} else {if($NF==0){if( $22 < 30000 && $27 < 20 && $26 <22 &&$31 < 25&& $37<15){print $0}}}}' ${train_data} > tmp && mv tmp ${train_data}

#sort 为了结果的label的 正负正确.
da && awk -F '\t' 'NR>1{if($NF==1){print $0}else{phone_list[$1]=$0}}END{for(k in phone_list){print phone_list[k]}}' $train_data > tmp && mv tmp $train_data
#dup
cat $train_data | awk 'BEGIN{pos_num=1500000;}{if($NF==1){for(i=0;i<70;i++){print $0;}} else if(pos_num>=0){ print $0;pos_num-=1}}' > ${train_data}_dup
cat ${train_data}_dup| awk '{print $NF}'|sort|uniq -c #校验数据

#变量定义d

#python脚本
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python reconstruct_conf.py
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python gen_feature_ids.py
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python fea_extra.py -m train
da && head ${features_lines}


#random
#awk 'BEGIN{srand()}{b[rand()NR]=$0}END{for(x in b)print b[x]}' ${features_lines} > whole_file

## check
head ${features_lines}
wc whole_file
tail -n 800000 whole_file > train_file
head -n 200000 whole_file > test_file

cat test_file| cut -b 1 > y_test
#train

da &&/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/train -v 5 -e 0.1 -s 7 -c 0.5  ${features_lines}
da && rm user_feature_model
/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/train  -e 0.1 -s 7 -c 0.5 /Users/dongjian/data/${features_lines} /Users/dongjian/data/user_feature_model
/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/predict -b 1 /Users/dongjian/data/test_file /Users/dongjian/data/user_feature_model /Users/dongjian/data/predict
cat ~/data/predict|tail -n+2 | cut -b 1  > ~/data/y_predict

paste y_test y_predict > rs
awk -F \t '{if($1==1 && $2==1 ){print 1}}' rs|wc -l > val # tp
awk -F \t '{if($1==0 && $2==0 ){print 1}}' rs|wc -l >> val # tn
awk -F \t '{if($1==0 && $2==1 ){print 1}}' rs|wc -l >> val # fp
awk -F \t '{if($1==1 && $2==0 ){print 1}}' rs|wc -l >> val # fn

#假证率 fn/tp+fn
awk  '{d[NR]=$0}END{print d[4]/(d[4]+d[1])}' val


# 构建正,负样本的测试文件.
awk -F '\t' '{if(NR==1 ||$NF == 1){print $0}}' user_feature_raw > user_feature_raw_label_1
awk -F '\t' '{if(NR==1 ||$NF == 0){print $0}}' user_feature_raw > user_feature_raw_label_0

#test
python /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector/fea_extra.py -m test
/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/predict -b 1 /Users/dongjian/data/${features_lines} /Users/dongjian/data/user_feature_model /Users/dongjian/data/user_features_predict

#print model
features_ids_name=${app}_features_ids
cat $features_ids_name >  user_features_features_ids_sort
tail -n+7 user_feature_model > model_xishu
awk -F '&#&' 'FNR==NR{flag[$2]=1;line[$2]=$0;next}{if(flag[FNR]==1){printf("%s\t%s\n",line[FNR],$0)}}' $features_ids_name model_xishu > model_xishu_ids
sort -t$'\t' -k 2gr model_xishu_ids > model_xishu_ids_sort_${version}

#tran id to name
#把feature_lines 转化为带特征name的
python /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector/tran_id_to_value.py #user_features_features_lines_with_info

#paste
da &&awk -F ' ' 'BEGIN{print "value"}{print $0}' ${features_lines}_with_info > user_features_value_for_paste #带上前缀value
da &&awk -F ' ' 'BEGIN{print "prob"}NR==1{if($2==0){num=3}else{num=2}}{if(NR!=1){print $num}}' user_features_predict> user_features_for_paste #带上前缀 prob
da &&paste user_features_for_paste $test_file > user_features_rs #将prob 和原始文件组合起来 user_features 为目标文件
da &&paste user_features_rs user_features_value_for_paste > ${features_lines}_final #将value 和原始文件组合起来

#select
#筛选阈值在xx以上的文件.
da &&awk -F '\t' '{if($1>0.75&&$1<0.8&&NR!=1){print $0}}' user_features_rs | sort -k 1 -r -n -g > user_features_rs_above_9_tmp && head -n 1 user_features_rs | cat - user_features_rs_above_9_tmp > user_features_rs_above_9_ttmp && mv user_features_rs_above_9_ttmp user_features_rs_bw_75_80
da &&awk -F '\t' '{if($1>0.9&&NR!=1){print $0}}' ${features_lines}_final | sort -k 1 -r -n -g > user_features_rs_above_9_tmp && head -n 1 user_features_rs | cat - user_features_rs_above_9_tmp > user_features_rs_above_9_ttmp && mv user_features_rs_above_9_ttmp user_features_rs_ab_9
da &&awk -F '\t' '{if($1>0.85&&NR!=1){print $0}}' ${features_lines}_final | sort -k 1 -r -n -g > user_features_rs_above_9_tmp && head -n 1 user_features_rs | cat - user_features_rs_above_9_tmp > user_features_rs_above_9_ttmp && mv user_features_rs_above_9_ttmp user_features_rs_ab_85
da &&awk -F '\t' '{if($1>0.9&&NR!=1){print $0}}' ${features_lines}_final | sort -k 1 -r -n -g > user_features_rs_above_9_tmp && head -n 1 user_features_rs | cat - user_features_rs_above_9_tmp > user_features_rs_above_9_ttmp && mv user_features_rs_above_9_ttmp ${features_lines}_rs_all
wc ${features_lines}_rs_all

#csv
tr -s "," "#" < ${features_lines}_rs_all |awk -F '\t' 'BEGIN{OFS=","}{$1=$1;print $0}'  > ${features_lines}_rs_prob.csv
(da&& awk 'BEGIN{srand()}{b[rand()NR]=$0}END{for(x in b)print b[x]}' ${features_lines}_rs_prob.csv > tmp && head -1 ${features_lines}_rs_prob.csv|cat - tmp > tmp_1 && head -n 1 tmp_1 && tail -n+2  tmp_1|tail -n 60|sort -t$'\t' -k 1rn) >  ${features_lines}_rs_prob_30.csv

#assist

## 查看训练集
tr -s ',' '#' < user_feature_raw |awk -F '\t' 'BEGIN{OFS=","}{$1=$1;if($NF==1 || NR ==1 ){print $0}}' |head -1000 > user_feature_raw_label_1.csv
tr -s ',' '#' < user_feature_raw |awk -F '\t' 'BEGIN{OFS=","}{$1=$1;if($NF==0 || NR ==1 ){print $0}}' |head -1000 > user_feature_raw_label_0.csv

#sort
tail -n+2 user_features_rs |sort -k 1 -r -n > user_features_rs_tmp && head -n 1 user_features_rs | cat - user_features_rs_tmp > user_features_rs_sort


#get line
head -1 user_features|awk -F " " '{for(i=1;i<=NF;i++){print $i}}' > tmp_0
cat user_features| grep 17858529893|awk -F " " '{for(i=1;i<=NF;i++){print $i}}' > tmp_1 && paste tmp_0 tmp_1 > tmp_rs && cat tmp_rs
