import sqlite3

from sumo.core.constants import *


def init_computeforce_db():
	"""Initiliazes connection to database	
	"""
	
	conn = sqlite3.connect("./" +DB_NAME)
 
	cursor = conn.cursor()
	 
	cursor.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME_POINTS + " (epoch integer, instance_id text, metric text, average double, maximum double, minimum double)")

	cursor.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME_SIGNALS + " (start_epoch integer, end_epoch integer, instance_id text, metric text, signal text)")

	conn.close()


def add_metric_signal_to_db(start, end, metric, instance_id, signal):
	"""Add signal of a particular metric and of particular instance to db.
		:param start: start time of signal.
		:type start: datetime.
		:param end: end time of signal.
		:type end: datetime.
		:param metric: metric's name.
		:type metric: string.
		:param instance_id: instance's id.
		:type instance_id: int.
		:param signal: metric's data.
		:type signal: dict.	
	"""
	
	conn = sqlite3.connect("./" +DB_NAME)
	cursor = conn.cursor()

	start_epoch = start.strftime("%s")
	end_epoch = end.strftime("%s")

	query = "INSERT INTO " + TABLE_NAME_SIGNALS + " (start_epoch, end_epoch, instance_id, metric, signal) VALUES(" +start_epoch+ ", "+ end_epoch+", '" +instance_id+ "', '" +metric+ "', \"" + str(signal) + "\")"

	cursor.execute(query)

	conn.commit()
	conn.close()
				

def get_metric_signals_from_db(start, end, metric, instance_id):
	"""Get signal of a particular metric and of particular instance from db.
		:param start: start time of signal.
		:type start: datetime.
		:param end: end time of signal.
		:type end: datetime.
		:param metric: metric's name.
		:type metric: string.
		:param instance_id: instance's id.
		:type instance_id: int.
		:param signal: metric's values.
		:type signal: list.	
		:returns: dict -- a dict of metric's data.
	"""
	
	conn = sqlite3.connect("./" +DB_NAME)
	cursor = conn.cursor()

	start_epoch = start.strftime("%s")
	end_epoch = end.strftime("%s")

	query = "SELECT * FROM " + TABLE_NAME_SIGNALS + " WHERE start_epoch > "+str(start_epoch)+ " AND end_epoch < "+str(end_epoch)+ " AND instance_id = '" +instance_id+ "' AND metric = '"+metric+"'"

	cursor.execute(query)
	data = cursor.fetchall()
	
	conn.close()

	metric_signals = eval(data)

	return metric_signals


def add_metric_datapoints_to_db(datapoints):
	"""Add metric's datapoints to db.
		:param datapoints: the datapoints.
		:type datapoints: datapoints.	
	"""
	signal = datapoints_to_signal(datapoints)

	db.init_computeforce_db()

	db.add_metric_signal_to_db(start, end, metric_name, instance_id, signal)
