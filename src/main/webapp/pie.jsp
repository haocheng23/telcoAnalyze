<%--
  Created by IntelliJ IDEA.
  User: hc
  Date: 2018/5/6
  Time: 16:51
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>畅销套餐</title>
</head>

<script src="js/jquery-2.1.4.min.js"></script>
<script src="js/echarts.min.js"></script>
<body>
<!-- 为ECharts准备一个具备大小（宽高）的Dom -->
<div id="main" style="height:500px;border:1px solid #ccc;padding:10px;"></div>
</body>
</html>
<script type="text/javascript">
    //初始化echarts实例
    var myChart = document.getElementById('main');
    myChart.setOption({
        title : {
            text: '套餐销量情况',
            subtext: '纯属虚构',
            x:'center'
        },
        tooltip : {
            trigger: 'item',
            formatter: "{a} <br/>{b} : {c} ({d}%)"
        },
        legend: {
            orient : 'vertical',
            x : 'left',
            data:[]
        },
        toolbox: {
            show : true,
            feature : {
                mark : {show: true},
                dataView : {show: true, readOnly: false},
                magicType : {
                    show: true,
                    type: ['pie', 'funnel'],
                    option: {
                        funnel: {
                            x: '25%',
                            width: '50%',
                            funnelAlign: 'left',
                            max: 1548
                        }
                    }
                },
                restore : {show: true},
                saveAsImage : {show: true}
            }
        },
        calculable : true,
        series : [
            {
                name:'访问来源',
                type:'pie',
                radius : '55%',
                center: ['50%', '60%'],
                data:[]
            }
        ]
    });
    myChart.showLoading();    //数据加载完之前先显示一段简单的loading动画

    var names = [];  //套餐类型
    var values = [];  //套餐销量
    //异步加载数据
    $.ajax({
        type : "post",
        async : true,       //异步请求
        url : "GetPieData",
        data : {},
        dataType : "json", //返回数据形式为json
        success : function(result) {
            //请求成功时执行该函数内容，result即为服务器返回的json对象
            if (result) {
                for(var i=0;i<result.length;i++){
                    //console.log(result[i].name);
                    names.push(result[i].taoCan);
                    values.push(result[i].total);
                }
                myChart.setOption({        //加载数据图表
                    legend: {
                        data: names
                    },
                    series: [{
                        // 根据名字对应到相应的系列
                        name: '销量',
                        data: values
                    }]
                });
            }

        },
        error : function(errorMsg) {
            alert("不好意思，大爷，图表请求数据失败!");
            myChart.hideLoading();
        }
    })

</script>
