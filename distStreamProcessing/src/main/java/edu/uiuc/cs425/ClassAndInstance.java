package edu.uiuc.cs425;

public class ClassAndInstance {
	
	private Class<?>			m_cClass;
	private Object				m_oClassObject;
	private int					m_nInstanceId;
	
	
	public Object getM_oClassObject() {
		return m_oClassObject;
	}
	public void setM_oClassObject(Object m_oClassObject) {
		this.m_oClassObject = m_oClassObject;
	}
	public Class<?> getM_cClass() {
		return m_cClass;
	}
	public void setM_cClass(Class<?> m_cClass) {
		this.m_cClass = m_cClass;
	}
	public int getM_nInstanceId() {
		return m_nInstanceId;
	}
	public void setM_nInstanceId(int m_nInstanceId) {
		this.m_nInstanceId = m_nInstanceId;
	}
	
}
