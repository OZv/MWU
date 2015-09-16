var vrh=(function(){return{
c:function(c){
var p=c.parentNode.parentNode;
var d=p.getElementsByTagName("div");
var r=/\w+(?=\.png$)/;
for(var i=0;i<d.length;i++)
if(d[i].style.display=="block"){c.src=c.src.replace(r,"x");d[i].style.display="none";}
else{c.src=c.src.replace(r,"c");d[i].style.display="block";}}
}}());
var dzp=(function(){
function e(t,c){
var d=document.getElementsByTagName("div");
for(var i=0;i<d.length;i++){
var p=d[i].previousSibling;
if(p&&(d[i].className=="cdw"||d[i].className=="oje")){
var h=p.offsetHeight;
if(p.childNodes[1].className=="izv"&&d[i].offsetHeight>h*12){
p.childNodes[1].style.backgroundPosition="0 0";
d[i].style.display="none";
}
}
}
}
function g(a,b,c,d){
var s=a.style;
s.backgroundColor=d;s.color=c;s.borderColor=c;
s=b.style;
s.backgroundColor=c;s.color="#FFF";s.borderColor=c;
}
if(typeof(dzp)=="undefined"){
if(window.addEventListener)
	window.addEventListener("load",e,false);
else window.attachEvent("onload",e);
}
return{
v:function(c,f){
c.removeAttribute("onclick");
with(c.style){
	cursor="default";outline="1px dotted gray";
}
var p="soundc11/";
if(/^span:/.test(f)){
	var l= f.replace(/^span:/,"");
	p="audio/prons/"+l[0]+"/"+l+".mp3";
}else{
	if(/^gg/.test(f))p+="gg";
	else if(/^bix/.test(f))p+="bix";
	else if(/^[0-9]/.test(f))p+="number";
	else p+=f[0];
	p+='/'+f+".wav";
}
var u="http://media.merriam-webster.com/"+p;
var b=function(){
	with(c.style){outline="";cursor="pointer";}
	c.setAttribute("onclick","dzp.v(this,'"+f+"')");
	};
var t=setTimeout(b,2000);
try{
with(document.createElement("audio")){
	setAttribute("src",u);
	onloadstart=function(){clearTimeout(t);};
	onended=b;
	play();
}
}
catch(e){
c.style.outline="";
}
},
x:function(c){
var n=c.parentNode.nextSibling;
if(n.style.display!="none"){
n.style.display="none";
c.style.backgroundPosition="0 0";
}else{
n.style.display="block";
c.style.backgroundPosition="-16px 0";
}
},
h:function(c){
var p=c.parentNode;
var d=p.nextSibling;
var t=d.nextSibling;
c.className="kfh";
c.setAttribute("onclick","javascript:void(0);");
var b=c.nextSibling?c.nextSibling:c.previousSibling;
var j=c.nextSibling?1:0;
b.className="dt7";
b.setAttribute("onclick","dzp.h(this)");
if(j)g(b,c,"#4AB0EF","#F5F7FB");
else g(b,c,"#F48040","#FDF9F7");
if(t.style.display!="block"){t.style.display="block";d.style.display="none";}
else{t.style.display="none";d.style.display="block";}}
}}());
