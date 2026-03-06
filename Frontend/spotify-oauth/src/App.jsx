import React, { useEffect } from 'react';
import { BrowserRouter as Router, Route, Routes, useNavigate } from 'react-router-dom';
import LoginPage from './LoginPage';
import PlaylistInputPage from './PlaylistInputPage';
import './App.css';

function App() {
    return (
        <Router>
            <div className="app">
                <Routes>
                    <Route path="/login" element={<LoginPage />} />
                    <Route path="/input-playlist" element={<PlaylistInputPage />} />
                    <Route path="*" element={<DefaultComponent />} />
                </Routes>
            </div>
        </Router>
    );
}

function DefaultComponent() {
    let navigate = useNavigate();

    useEffect(() => {
        navigate('/login');
    }, [navigate]);

    return null;
}

export default App;
