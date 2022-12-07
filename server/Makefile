DATA_SERVER_FILES = ConDBServerApp.py app.py

GUI_SERVER_FILES = DataBrowser.py \
    index.html table_index.html show_table.html  template.html table_data.json plot_table.html

build: 
	cp $(DATA_SERVER_FILES) $(GUI_SERVER_FILES) $(SRVROOT)
	cd $(SRVROOT);	echo "GUI_Version = \"$(GUI_VERSION)\"" > GUI_Version.py
