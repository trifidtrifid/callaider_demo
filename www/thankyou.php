<?


$message = '<html><head>
 <title>Зпрос на демонстрацию CallAider</title>
</head>
<body>
<p>New request fot CallAider demo</p>
<p> from: '.$_POST['nameinput'].'
<br/>email: '.$_POST['Email'].'
<br/>company: '.$_POST['companyinput'].'
<br/>phone: '.$_POST['phoneinput'].'
<br/>comment: '.$_POST['comment'].'
</p>
</body>
</html>';

$message = wordwrap($message,70);

$headers  = "Content-type: text/html; charset=utf-8 \r\n";
$headers .= "From: admin@atalassoftware.com\r\n";
$headers .= "Bcc: ceo@atalassoftware.com\r\n";
$headers .= "To: alexey.chervyakov@gmail.com\r\n";
$headers .= "From: trifid@gmail.com\r\n";
$newsubject = '=?UTF-8?B?'.base64_encode('CallAider demo request from '.$_POST['nameinput'].'  @  '.$_POST['companyinput']).'?=';

if( mail($_POST['to'], $newsubject, $message, $headers)){
      #  echo 'OK'.' HEADers:'.$headers.' subj: '.$newsubject;
} else {
      #  echo 'FAIL'.'HEADers:'.$headers.' subj: '.$newsubject;
}
?>


<!DOCTYPE html>
<html lang="en">
	<head>
		<meta charset="utf-8">
		<meta http-equiv="X-UA-Compatible" content="IE=edge">
		<meta name="viewport" content="width=device-width, initial-scale=1.0">
		<meta name="description" content="CallAider аналитическая платформа медиатор для контроля  качественных характеристик ( KPI ) и антифрод ( antifraud )  для голосового сервиса, VAS сервисов ( SMS, LBS, MMS,VoIP, IVR,IP, SS7, SIGTRAN )">
		<meta name="author" content="Atalassofrware.com">
		<link href="assets/img/favicon.png" rel="shortcut icon"  >
		<title>CallAider. Спасибо, за ваш интерес.</title>

		<!-- Bootstrap core CSS -->
		<link href="assets/css/bootstrap.min.css" rel="stylesheet">
		<!-- Plugins -->
		<link href="assets/css/social-icons.css" rel="stylesheet">
		<link href="assets/css/icomoon.css" rel="stylesheet">
		<!-- Custom styles for this template -->
		<link href="assets/css/style.css" rel="stylesheet">
		<!-- Web Fonts -->
		<!--link href='//fonts.googleapis.com/css?family=Lato:300,400,700,300italic,400italic' rel='stylesheet' type='text/css'-->

		<!-- HTML5 Shim and Respond.js IE8 support of HTML5 elements and media queries -->
		<!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
		<!--[if lt IE 9]>
			<script src="https://oss.maxcdn.com/libs/html5shiv/3.7.0/html5shiv.js"></script>
			<script src="https://oss.maxcdn.com/libs/respond.js/1.4.2/respond.min.js"></script>
		<![endif]-->
		<script src="assets/js/modernizr.custom.js"></script>
	</head>

	<body data-spy="scroll" data-offset="0" data-target="#navigation">

		<!-- Fixed navbar -->
		<nav id="navigation" class="navbar navbar-default navbar-fixed-top" role="navigation">
			<div class="container">
				<!-- Brand and toggle get grouped for better mobile display -->
				<div class="navbar-header">
					<button type="button" class="navbar-toggle" data-toggle="collapse" data-target="#bs-navbar-collapse">
						<span class="icon-bar"></span>
						<span class="icon-bar"></span>
						<span class="icon-bar"></span>
					</button>
					<a class="navbar-brand" href="index.html"><span class="icon-stack"></span> <b>ATALAS INC.</b></a>
				</div>

				<!-- Collect the nav links, forms, and other content for toggling -->
				<div class="navbar-collapse collapse" id="bs-navbar-collapse">
					<ul class="nav navbar-nav">
						<li class="active"><a href="index.html" class="smoothScroll">Главная</a></li>
						<li><a href="how_it_work.html" class="smoothScroll">Как это работает</a></li>
						<li class="dropdown">
				                <a href="solutions.html" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false">Решения <span class="caret"></span></a>
				                <ul class="dropdown-menu" >
				                  <li><a href="solutions.html#FMS">CallAider FMS</a></li>
				                  <li><a href="solutions.html#voice">Мониторинг голосового сервиса</a></li>
				                  <li><a href="solutions.html#SMS">Мониторинг  сервиса SMS</a></li>
				                  <li><a href="solutions.html#LBS">Мониторинг LBS</a></li>
				                </ul>
              				</li>
						<li><a href="company.html" class="smoothScroll">О Компании</a></li>
					</ul>

					<!-- Sign In / Sign Up -->
					<ul class="nav navbar-nav navbar-right">
						<div class="navbar-form pull-left">
							<a href="#signup" type="button" class="btn btn-sm btn-theme" data-toggle="modal" data-target=".bs-modal-sm">Попробовать сейчас>></a>
						</div>
					</ul>
				</div><!--/nav-collapse -->
			</div><!-- /container -->
		</nav><!-- /fixed-navbar -->


		<div class="modal fade bs-modal-sm" id="myModal" tabindex="-1" role="dialog" aria-labelledby="mySmallModalLabel" aria-hidden="true">
			<div class="modal-dialog modal-sm">
				<div class="modal-content">
					<br>
					<div class="modal-body">
						<form class="form-horizontal" action="thankyou.php" method="POST">
							<fieldset>
								<!-- Text input-->
								<div class="control-group">
									<label class="control-label" for="Email">* Email:</label>
									<div class="controls">
										<input required id="Email" name="Email" class="form-control input-large" type="text" placeholder="Укажите Вашу Email" >
									</div>
								</div>

								<div class="control-group">
									<label class="control-label" for="nameinput">* Имя:</label>
									<div class="controls">
										<input required id="nameinput" name="nameinput" class="form-control input-medium" type="text" placeholder="Укажите Ваше имя">
									</div>
								</div>

								<div class="control-group">
									<label class="control-label" for="companyinput">* Компания:</label>
									<div class="controls">
										<input required id="companyinput" name="companyinput" class="form-control input-medium" type="text" placeholder="Укажите Вашу компанию">
									</div>
								</div>

								<div class="control-group">
									<label class="control-label" for="phoneinput">Телефон:</label>
									<div class="controls">
										<input id="phoneinput" name="phoneinput" class="form-control input-medium" type="text" placeholder="Укажите контактный телефон">
									</div>
								</div>
								<div class="control-group">
									<label class="control-label" for="comment">Комментарий:</label>
									<div class="controls">
										<textarea id="comment" name="comment" class="form-control input-medium" style="height: 100px; resize: none;" type="text" placeholder="Ваш комментарий"></textarea>
									</div>
								</div>

								<!-- Button -->
								<div class="control-group">
									<label class="control-label" for="signin"></label>
									<div class="controls">
										<button id="signin" name="signin" class="btn btn-theme btn-block">Получить</button>
									</div>
								</div>
							</fieldset>
						</form>
					</div><!-- /modal-body -->

					<!--
                    <div class="modal-footer">
						<div class="text-center">
							<button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
						</div>
					</div>
                    -->
				</div><!-- /modal-content -->
			</div><!-- /modal-dialog -->
		</div><!-- /modal -->


		<!-- Header Wrap -->
		<section id="home">
			<div class="headerwrap" style="height: 300px;">
				<div class="container">
					<div class="row text-center">
						<div class="col-lg-12">
							<br>
							<h1><b>Спасибо!</b></h1>
							<br>
						</div>

					</div>
				</div> <!-- /container -->
			</div><!-- /headerwrap -->


			<!-- Intro Wrap -->
			<div class="intro">
				<div class="container">
					<div class="row text-center">
						<h2>Мы свяжемся с Вами в ближайшее время!</h2>
						<hr>
						<br>
					</div>
					<br>
					<br>
					<br>
					<br>
					<br>
					<br>
					<br>
					<br>
					<br>
					<br>
					<br>
				</div> <!-- /container -->
			</div><!-- /introwrap -->
		</section>


		<!-- Copyright Wrap -->
		<div class="copywrap">
			<div class="container">
				<div class="row">
					<div class="col-lg-10">
						<p>Copyright &copy;2015 Atalas Inc. All Rights Reserved.</p>
					</div>
					<div class="col-lg-2">
						<p><a href="#">Terms</a> | <a href="#">Privacy</a></p>
					</div>
				</div><!-- /row -->
			</div><!-- /container -->
		</div><!-- /copywrap (copyright) -->


		<!-- jQuery (necessary for Bootstrap's JavaScript plugins) -->
		<script src="assets/js/jquery-2.1.1.min.js"></script>
		<!-- Include all compiled plugins (below), or include individual files as needed -->
		<script src="assets/js/bootstrap.min.js"></script>
		<script src="assets/js/jquery.easing.1.3.js"></script>
		<script src="assets/js/detectmobilebrowser.js"></script>
		<script src="assets/js/smoothscroll.js"></script>
		<script src="assets/js/waypoints.js"></script>
		<script src="assets/js/main.js"></script>
		<script>
			$('.carousel').carousel({
				interval: 3500,
				pause: "none"
			})
		</script>
		<!-- Yandex.Metrika counter --><script type="text/javascript"> (function (d, w, c) { (w[c] = w[c] || []).push(function() { try { w.yaCounter32214144 = new Ya.Metrika({ id:32214144, clickmap:true, trackLinks:true, accurateTrackBounce:true, webvisor:true, trackHash:true }); } catch(e) { } }); var n = d.getElementsByTagName("script")[0], s = d.createElement("script"), f = function () { n.parentNode.insertBefore(s, n); }; s.type = "text/javascript"; s.async = true; s.src = "https://mc.yandex.ru/metrika/watch.js"; if (w.opera == "[object Opera]") { d.addEventListener("DOMContentLoaded", f, false); } else { f(); } })(document, window, "yandex_metrika_callbacks");</script><noscript><div><img src="https://mc.yandex.ru/watch/32214144" style="position:absolute; left:-9999px;" alt="" /></div></noscript><!-- /Yandex.Metrika counter -->
		<!-- Google Analytics counter --><script>  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)  })(window,document,'script','//www.google-analytics.com/analytics.js','ga');  ga('create', 'UA-16858078-1', 'auto');  ga('send', 'pageview');</script><!-- Emd of Goofle analytics counter -->
	</body>
</html>
