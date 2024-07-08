// src/components/SidebarRight.js
import React, { useState, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { suggestUsers, popularMovies, suggestMoviesBasedOnRandom, getGenresBasedOnPopularity } from './api';

const buttonStyle = {
  backgroundColor: '#4caf50',
  color: 'white',
  padding: '5px 10px',
  border: 'none',
  borderRadius: '5px',
  cursor: 'pointer',
  margin: '0 5px',
};

function SidebarRight() {
  const location = useLocation();
  const userData = location.state ? location.state : null;
  const [listUsers, setUsers] = useState(null);
  const [listPopMovies, setPopMovies] = useState(null);
  const [listSugMoviesBasedOnRandom, setSugMoviesBasedOnRandom] = useState(null);
  const [listGenresBasedOnPopularity, setGenresBasedOnPopularity] = useState(null);

  const handleUserSuggest = async () => {
    try {
      const response = await suggestUsers(userData.user.name);
      setUsers(response);
    } catch (error) {
      console.error('Error during login:', error);
    }
  };
  const handlePopularMovies = async () => {
    try {
      const response = await popularMovies(userData.user.name);
      setPopMovies(response);
    } catch (error) {
      console.error('Error during login:', error);
    }
  };

  const handleSuggestMoviesBasedOnRandom = async () => {
    try {
      const response = await suggestMoviesBasedOnRandom(userData.user.name);
      setSugMoviesBasedOnRandom(response);
    } catch (error) {
      console.error('Error during login:', error);
    }
  };

  const handleGenresBasedOnPopularity = async () => {
    try {
      const response = await getGenresBasedOnPopularity(userData.user.name);
      setGenresBasedOnPopularity(response);
    } catch (error) {
      console.error('Error during login:', error);
    }
  };

  useEffect(() => {
    // Call handleUserSuggest when the component mounts
    handleUserSuggest();
    handlePopularMovies();
    handleSuggestMoviesBasedOnRandom();
    handleGenresBasedOnPopularity();
  }, []);

  return (
    <div className="sidebar sidebar-right">

      <h2>I feel lucky</h2>
      {listSugMoviesBasedOnRandom !== null ? (
        listSugMoviesBasedOnRandom.length > 0 ? (
          <ul className="user-list">
            {listSugMoviesBasedOnRandom.map((user, index) => (
              <li key={index} className="user-item">
                {user.Movie} ({user.Year}) <br />
              </li>
            ))}
          </ul>
        ) : (
          <p>No suggested movies found.</p>
        )
      ) : (
        <p>Loading...</p>
      )}
      <h2>Connect with Friends</h2>
      {listUsers !== null ? (
        listUsers.length > 0 ? (
          <ul className="user-list">
            {listUsers.map((user, index) => (
              <li key={index} className="user-item">
                {user.Friend_suggestion} <br /> Movies in common: {user.n}
                <button style={buttonStyle}>
                Send Friend Request
              </button>
              </li>
            ))}
          </ul>
        ) : (
          <p>No suggested users found.</p>
        )
      ) : (
        <p>Loading...</p>
      )}
      <h2>Featured Content</h2>
      {listPopMovies !== null ? (
        listPopMovies.length > 0 ? (
          <ul className="user-list">
            {listPopMovies.map((user, index) => (
              <li key={index} className="user-item">
                {user.Movie} ({user.Year})
              </li>
            ))}
          </ul>
        ) : (
          <p>No popular movies found.</p>
        )
      ) : (
        <p>Loading...</p>
      )}
      {/* Add sidebar content as needed */}
      <h2>Popular Genres</h2>
      {listGenresBasedOnPopularity !== null ? (
        listGenresBasedOnPopularity.length > 0 ? (
          <ul className="user-list">
            {listGenresBasedOnPopularity.map((user, index) => (
              <li key={index} className="user-item">
                {user.Genre} ({user.ReviewCount})
              </li>
            ))}
          </ul>
        ) : (
          <p>No popular genres found.</p>
        )
      ) : (
        <p>Loading...</p>
      )}
    </div>
  );
}

export default SidebarRight;
